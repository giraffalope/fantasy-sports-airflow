import glob
import logging
import random
import time
from datetime import datetime
from pathlib import Path

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_utils import MySqlTemplateFileExecutor, MySQLTemplateFileOperator
from airflow_utils.wait_operator import WaitOperator
from include.nascar.data_sources import (
    DEFAULT_NASCAR_DATA_SOURCE,
    RACING_REFERENCE_USER_AGENTS,
    cup_series_convert_year_schedule_html_to_csv,
    cup_series_download_year_schedule_html,
)
from include.nascar.data_sources.base import NascarScrapeSource
from include.nascar.data_sources.racing_reference import (
    download_entrylist_html_if_missing,
    download_loop_data_html_if_missing,
    download_penalties_html_if_missing,
    download_practices_html_if_missing,
    download_qualifying_html_if_missing,
)
from include.nascar.enums import NascarSeries
from include.utils.file import File
from include.utils.scraper import Scraper

# Swap this object (or build from Airflow Variable) to use another scrape source implementation.
NASCAR_DATA_SOURCE: NascarScrapeSource = DEFAULT_NASCAR_DATA_SOURCE


def create_tracks_if_not_exists(
    series: NascarSeries, source: NascarScrapeSource = NASCAR_DATA_SOURCE, **kwargs
):
    # Get the MySQL (MariaDB) connection from Airflow
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    source_id = getattr(source, "id", "racing_reference")
    source_url = getattr(source, "base_url", None)

    hook.run(
        """
        UPDATE `nascar`.`data_sources`
        SET `url` = %s, `deleted_at` = NULL
        WHERE `name` = %s
        """,
        parameters=(source_url, source_id),
    )
    hook.run(
        """
        INSERT INTO `nascar`.`data_sources` (`name`, `url`)
        SELECT %s, %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM `nascar`.`data_sources` ds
            WHERE ds.`name` = %s
        )
        """,
        parameters=(source_id, source_url, source_id),
    )

    year = kwargs["data_interval_start"].year
    base = source.storage_path_series_year(series.db_name, year)
    rows = File.read_csv(filename=f"{base}/schedule.csv", as_dict=True)

    for row in rows:
        logging.info(str(row))
        track_name = row.get("week_track")
        hook.run(
            """
            INSERT INTO `nascar`.`data_sources_tracks` (`data_source_id`, `name`)
            SELECT ds.`id`, %s
            FROM `nascar`.`data_sources` ds
            WHERE ds.`name` = %s
                AND ds.`deleted_at` IS NULL
            ON DUPLICATE KEY UPDATE `deleted_at` = NULL
            """,
            parameters=(track_name, source_id),
        )
        select_sql = """
            SELECT COUNT(*) AS total_matches
            FROM `nascar`.`tracks` t
            WHERE TRIM(t.`name`) = TRIM(%s)
                AND t.`deleted_at` IS NULL
            ;
        """
        results = hook.get_records(select_sql, parameters=(track_name,))
        # Each result row is a tuple
        for result in results:
            total_matches = int(result[0])
            logging.info(f'Number of tracks named "{track_name}": {str(total_matches)}')
            if total_matches == 0:
                hook.run(
                    """
                    INSERT IGNORE INTO `nascar`.`tracks` (`name`)
                    VALUES (%s)
                    ;
                    """,
                    parameters=(track_name,),
                )
                logging.warning(
                    f'Additional data needed in `nascar`.`tracks` for new record "{track_name}"'
                )
    hook.run(
        """
        UPDATE `nascar`.`data_sources_tracks` dst
        JOIN `nascar`.`data_sources` ds
            ON ds.`id` = dst.`data_source_id`
            AND ds.`name` = %s
            AND ds.`deleted_at` IS NULL
        INNER JOIN `nascar`.`tracks` t
            ON TRIM(t.`name`) = TRIM(dst.`name`)
            AND t.`deleted_at` IS NULL
        SET dst.`track_id` = t.`id`
        WHERE dst.`track_id` IS NULL
            AND dst.`deleted_at` IS NULL
        """,
        parameters=(source_id,),
    )


def create_weeks_if_not_exists(
    series: NascarSeries, source: NascarScrapeSource = NASCAR_DATA_SOURCE, **kwargs
):
    # Get the MySQL (MariaDB) connectionf from Airflow
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    source_id = getattr(source, "id", "racing_reference")

    year = kwargs["data_interval_start"].year
    base = source.storage_path_series_year(series.db_name, year)
    conv = source.converters

    select_sql = """
            SELECT ss.series_id AS series_id, ss.id AS season_id
            FROM nascar.seasons ss
            INNER JOIN nascar.series sr
                ON sr.id = ss.series_id
                AND sr.deleted_at IS NULL
            WHERE ss.series_id = %s
                AND ss.deleted_at IS NULL
                AND ss.year = %s
            ;
        """
    season_results = hook.get_records(select_sql, parameters=(series.series_id, year))
    # Each result row is a tuple
    for season_result in season_results:
        season_id = int(season_result[1])
        rows = File.read_csv(filename=f"{base}/schedule.csv", as_dict=True)
        for row in rows:
            logging.info(str(row))
            week_number = int(row.get("week_number"))
            week_url = row.get("week_url")
            week_date = row.get("week_date")
            week_track = row.get("week_track")
            week_race_name = row.get("week_race_name")
            # Get the track id of the track for the current row's race
            select_sql = """
                SELECT t.id AS track_id
                FROM nascar.tracks t
                INNER JOIN nascar.data_sources_tracks dst
                    ON dst.track_id = t.id
                    AND dst.deleted_at IS NULL
                INNER JOIN nascar.data_sources ds
                    ON ds.id = dst.data_source_id
                    AND ds.name = %s
                    AND ds.deleted_at IS NULL
                WHERE TRIM(dst.name) = TRIM(%s)
                    AND t.deleted_at IS NULL
                LIMIT 1
                ;
            """
            track_result = hook.get_first(select_sql, parameters=(source_id, week_track))
            # For each track that match the current row's track name
            track_id = int(track_result[0])
            # Get the week id, track id, if the week's race has been completed and total number of matches for the given season, week number and track id
            select_sql = """
                SELECT w.id AS week_id, w.track_id AS track_id,
                    (MAX(rr.id IS NOT NULL)) AS is_completed,
                    COUNT(*) AS total_matches
                FROM nascar.weeks w
                LEFT JOIN nascar.races r
                    ON w.id = r.week_id
                    AND r.deleted_at IS NULL
                LEFT JOIN nascar.race_results rr
                    ON r.id = rr.race_id
                    AND rr.deleted_at IS NULL
                WHERE w.season_id = %s
                    AND w.number = %s
                    AND w.track_id = %s
                    AND w.deleted_at IS NULL
                GROUP BY w.id, w.track_id
                LIMIT 1
                ;
            """
            week_result = hook.get_first(select_sql, parameters=(season_id, week_number, track_id))
            week_id = None
            week_is_race_completed = False
            if week_result:
                week_id = week_result[0]
                week_is_race_completed = week_result[2] == 1 or week_result[2] is True
            # If there are no weeks for the given season, week number and track id
            else:
                # Create the week
                insert_sql = """
                    INSERT nascar.weeks (season_id, track_id, number, race_name, scheduled_start)
                    VALUES (
                        %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                    season_id = VALUES(season_id),
                    track_id = VALUES(track_id),
                    number = VALUES(number),
                    race_name = VALUES(race_name),
                    scheduled_start = VALUES(scheduled_start)
                    ;
                """
                hook.run(
                    insert_sql,
                    parameters=(season_id, track_id, week_number, week_race_name, week_date),
                )
                # Find the newly created week's id
                sql = """
                    SELECT w.id
                    FROM nascar.weeks w
                    WHERE w.season_id = %s
                        AND w.track_id = %s
                        AND w.number = %s
                        AND w.race_name = %s
                        AND w.scheduled_start = %s
                        AND w.deleted_at IS NULL
                    LIMIT 1;
                """
                record = hook.get_first(
                    sql, parameters=(season_id, track_id, week_number, week_race_name, week_date)
                )
                week_id = record[0]
            # If the week does not have a recorded race
            if week_is_race_completed is False and week_url is not None and week_url != "":
                logging.info(
                    f"No race recorded for {series.db_name} series {str(year)} week {week_number}"
                )
                logging.info("Downloading files and attempting to insert available data...")
                # Follow the week_url
                base_path = source.storage_path_week(series.db_name, year, week_number)
                scraper = Scraper(base_path)
                time.sleep(random.randint(2, 5))
                resolved_week_url = source.resolve_page_url(week_url)
                if not resolved_week_url:
                    logging.warning("Could not resolve week_url; skipping downloads for this week")
                    continue
                # Download race results page
                scraper.download(
                    url=resolved_week_url,
                    filename="race-results.html",
                    user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
                )
                race_results_html = f"{base_path}/race-results.html"
                conv.convert_race_results(race_results_html, f"{base_path}/race-results.csv")
                conv.convert_prerace_penalties(
                    race_results_html, f"{base_path}/pre-race-penalties.csv"
                )
                conv.convert_caution_flags(race_results_html, f"{base_path}/caution-flags.csv")
                conv.convert_lap_leaders(race_results_html, f"{base_path}/lap-leaders.csv")
                download_entrylist_html_if_missing(
                    source,
                    series.db_name,
                    year,
                    week_number,
                    race_results_html,
                )
                entrylist_html_path = f"{base_path}/entrylist.html"
                entrylist_csv_path = f"{base_path}/entrylist.csv"
                if Path(entrylist_html_path).is_file():
                    conv.convert_entrylist(entrylist_html_path, entrylist_csv_path)
                practice_html_file_paths = download_practices_html_if_missing(
                    source,
                    series.db_name,
                    year,
                    week_number,
                    race_results_html,
                )
                conv.convert_practices(practice_html_file_paths)
                qualifying_html_file_path = download_qualifying_html_if_missing(
                    source,
                    series.db_name,
                    year,
                    week_number,
                    race_results_html,
                )
                conv.convert_qualifying(qualifying_html_file_path)
                loop_data_html_file_path = download_loop_data_html_if_missing(
                    source,
                    series.db_name,
                    year,
                    week_number,
                    race_results_html,
                )
                conv.convert_loop_data(loop_data_html_file_path)
                penalties_html_file_path = download_penalties_html_if_missing(
                    source,
                    series.db_name,
                    year,
                    week_number,
                    race_results_html,
                )
                conv.convert_penalties(penalties_html_file_path)

                hook = MySqlTemplateFileExecutor(
                    mysql_conn_id="nascar_conn", parameters={"week_id": week_id}
                )

                if Path(entrylist_csv_path).is_file():
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent
                        / "sql"
                        / "load_entrylist_data.sql",
                        context={
                            "week_id": week_id,
                            "series_id": series.series_id,
                            "entrylist_csv_file_path": entrylist_csv_path,
                        },
                    )

                practice_files = glob.glob(f"{base_path}/practice-*.csv")
                number_of_practices = len(practice_files)
                for i in range(1, number_of_practices + 1):
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent
                        / "sql"
                        / "load_practice_data.sql",
                        context={
                            "week_id": week_id,
                            "series_id": series.series_id,
                            "number": i,
                            "practice_csv_file_path": practice_files[i - 1],
                        },
                    )

                if len(glob.glob(f"{base_path}/qualifying.csv")) > 0:
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent
                        / "sql"
                        / "load_qualifying_data.sql",
                        context={
                            "week_id": week_id,
                            "series_id": series.series_id,
                            "qualifying_csv_file_path": f"{base_path}/qualifying.csv",
                        },
                    )

                if len(glob.glob(f"{base_path}/race-results.csv")) > 0:
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent
                        / "sql"
                        / "load_race_data.sql",
                        context={
                            "week_id": week_id,
                            "series_id": series.series_id,
                            "race_results_csv_file_path": f"{base_path}/race-results.csv",
                            "race_loop_data_csv_file_path": f"{base_path}/loop-data.csv",
                        },
                    )

                hook.run_templated_sql_file(
                    file_path=Path(__file__).parent
                    / "sql"
                    / "load_clean_up.sql",
                    context={
                        "week_id": week_id,
                    },
                )

            else:
                if week_result is not None:
                    total_matches = int(week_result[3])
                    logging.info(f"Number of matching weeks: {str(total_matches)}")
                else:
                    logging.info("No new data to load.")


with DAG(
    "nascar_scraper",
    start_date=datetime(2025, 7, 11),
    schedule="0 */4 * * *",
    catchup=True,
    max_active_runs=1,
) as dag:
    wait_random_time_task = WaitOperator(
        task_id="wait_random_time",
        seconds=random.randint(0, 5),  # 0 to 30 minutes
    )

    cup_series_download_year_schedule_html_task = PythonOperator(
        task_id="cup_series_download_year_schedule_html",
        python_callable=cup_series_download_year_schedule_html,
    )

    cup_series_convert_year_schedule_html_to_csv_task = PythonOperator(
        task_id="cup_series_convert_year_schedule_html_to_csv",
        python_callable=cup_series_convert_year_schedule_html_to_csv,
    )

    cup_series_create_season_if_not_exists_task = MySQLTemplateFileOperator(
        task_id="cup_series_create_season_if_not_exists",
        sql_file_path=Path(__file__).parent
        / "sql"
        / "create_season_if_not_exists.sql",
        mysql_conn_id="nascar_conn",
        parameters={"series_name": NascarSeries.CUP.db_name},
    )

    cup_series_create_tracks_if_not_exists_task = PythonOperator(
        task_id="cup_series_create_tracks_if_not_exists",
        python_callable=create_tracks_if_not_exists,
        op_kwargs={"series": NascarSeries.CUP},
    )

    cup_series_create_weeks_if_not_exists_task = PythonOperator(
        task_id="cup_series_create_weeks_if_not_exists",
        python_callable=create_weeks_if_not_exists,
        op_kwargs={"series": NascarSeries.CUP},
    )

    (
        wait_random_time_task
        >> cup_series_download_year_schedule_html_task
        >> cup_series_convert_year_schedule_html_to_csv_task
        >> cup_series_create_season_if_not_exists_task
        >> cup_series_create_tracks_if_not_exists_task
        >> cup_series_create_weeks_if_not_exists_task
    )
