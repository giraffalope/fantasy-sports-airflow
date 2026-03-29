from datetime import datetime
from pathlib import Path
import glob
import random
import logging
import time

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from airflow_utils.wait_operator import WaitOperator
from airflow_utils import MySQLTemplateFileOperator
from airflow_utils import MySqlTemplateFileExecutor

from include.utils.scraper import Scraper
from include.utils.file import File

from bs4 import BeautifulSoup

from include.nascar.data_sources import DEFAULT_NASCAR_DATA_SOURCE
from include.nascar.data_sources.base import NascarScrapeSource

USER_AGENTS = [
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    # "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"
]

# Swap this object (or build from Airflow Variable) to use another scrape source implementation.
NASCAR_DATA_SOURCE: NascarScrapeSource = DEFAULT_NASCAR_DATA_SOURCE


def download_year_schedule_html(context, series_name, source: NascarScrapeSource = NASCAR_DATA_SOURCE):
    year = context["data_interval_start"].year
    scraper = Scraper(source.storage_path_series_year(series_name, year))
    scraper.download(
        url=source.schedule_url(year),
        filename="schedule.html",
        user_agent=random.choice(USER_AGENTS)
    )

def cup_series_download_year_schedule_html(**context):
    download_year_schedule_html(context=context, series_name='Cup')

def convert_year_schedule_html_to_csv(context, series_name, source: NascarScrapeSource = NASCAR_DATA_SOURCE):
    year = context["data_interval_start"].year
    base = source.storage_path_series_year(series_name, year)
    html_file_path = f"{base}/schedule.html"
    csv_file_path = f"{base}/schedule.csv"
    source.converters.convert_schedule(html_file_path, csv_file_path)

def cup_series_convert_year_schedule_html_to_csv(**context):
    convert_year_schedule_html_to_csv(context=context, series_name='Cup')

def create_tracks_if_not_exists(series_name, source: NascarScrapeSource = NASCAR_DATA_SOURCE, **kwargs):
    # Get the MySQL (MariaDB) connection from Airflow
    hook = MySqlHook(mysql_conn_id="nascar_conn")

    year = kwargs["data_interval_start"].year
    base = source.storage_path_series_year(series_name, year)
    rows = File.read_csv(filename=f"{base}/schedule.csv", as_dict=True)

    for row in rows:
        logging.info(str(row))
        track_name = row.get("week_track")
        hook.run(
            """
            INSERT IGNORE INTO `nascar`.`racing_reference_tracks` (`name`)
            VALUES (%s)
            """,
            parameters=(track_name,),
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
            logging.info(f"Number of tracks named \"{track_name}\": {str(total_matches)}")
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
                    f"Additional data needed in `nascar`.`tracks` for new record \"{track_name}\""
                )
    hook.run(
        """
        UPDATE `nascar`.`racing_reference_tracks` rrt
        INNER JOIN `nascar`.`tracks` t
            ON TRIM(t.`name`) = TRIM(rrt.`name`)
            AND t.`deleted_at` IS NULL
        SET rrt.`track_id` = t.`id`
        WHERE rrt.`track_id` IS NULL
            AND rrt.`deleted_at` IS NULL
        """
    )

def download_entrylist_html(source, html_file_path, series_name, year, week_number):
    with open(html_file_path, "r") as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
    url = source.extract_entrylist_url(soup)
    if not url:
        logging.warning("No entry list link found on race results page")
        return
    scraper = Scraper(source.storage_path_week(series_name, year, week_number))
    time.sleep(random.randint(2, 5))
    scraper.download(
        url=url,
        filename="entrylist.html",
        user_agent=random.choice(USER_AGENTS)
    )

def download_practices(source, html_file_path, series_name, year, week_number):
    html_file_paths = list()
    with open(html_file_path, "r") as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
    url = source.extract_practice_results_url(soup)
    if url is None:
        return html_file_paths
    week_path = source.storage_path_week(series_name, year, week_number)
    scraper = Scraper(week_path)
    time.sleep(random.randint(2, 5))
    scraper.download(
        url=url,
        filename="practice-01.html",
        user_agent=random.choice(USER_AGENTS)
    )
    first_practice_html = f"{week_path}/practice-01.html"
    html_file_paths.append(first_practice_html)
    with open(first_practice_html, "r") as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
    logging.info(soup.find('div', {'class': 'entry-content'}))
    for number, session_url in source.iter_practice_session_urls(soup):
        scraper = Scraper(week_path)
        time.sleep(random.randint(2, 5))
        scraper.download(
            url=session_url,
            filename=f"practice-{number:02d}.html",
            user_agent=random.choice(USER_AGENTS)
        )
        html_file_paths.append(f"{week_path}/practice-{number:02d}.html")
    return html_file_paths

def download_qualifying(source, html_file_path, series_name, year, week_number):
    with open(html_file_path, "r") as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
    url = source.extract_qualifying_url(soup)
    if url is None:
        return None
    scraper = Scraper(source.storage_path_week(series_name, year, week_number))
    time.sleep(random.randint(2, 5))
    scraper.download(
        url=url,
        filename="qualifying.html",
        user_agent=random.choice(USER_AGENTS)
    )
    return f"{source.storage_path_week(series_name, year, week_number)}/qualifying.html"

def download_loop_data(source, html_file_path, series_name, year, week_number):
    with open(html_file_path, "r") as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
    url = source.extract_loopdata_url(soup)
    if url is None:
        return None
    scraper = Scraper(source.storage_path_week(series_name, year, week_number))
    time.sleep(random.randint(2, 5))
    scraper.download(
        url=url,
        filename="loop-data.html",
        user_agent=random.choice(USER_AGENTS)
    )
    return f"{source.storage_path_week(series_name, year, week_number)}/loop-data.html"

def download_penalties(source, html_file_path, series_name, year, week_number):
    with open(html_file_path, "r") as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
    url = source.extract_penalties_url(soup)
    if url is None:
        return None
    scraper = Scraper(source.storage_path_week(series_name, year, week_number))
    time.sleep(random.randint(2, 5))
    scraper.download(
        url=url,
        filename="penalties.html",
        user_agent=random.choice(USER_AGENTS)
    )
    return f"{source.storage_path_week(series_name, year, week_number)}/penalties.html"

def create_weeks_if_not_exists(series_name, source: NascarScrapeSource = NASCAR_DATA_SOURCE, **kwargs):
    # Get the MySQL (MariaDB) connectionf from Airflow
    hook = MySqlHook(mysql_conn_id="nascar_conn")

    year = kwargs["data_interval_start"].year
    base = source.storage_path_series_year(series_name, year)
    conv = source.converters

    select_sql = """
            SELECT ss.series_id AS series_id, ss.id AS season_id
            FROM nascar.series sr
            INNER JOIN nascar.seasons ss
                ON sr.id = ss.series_id
                AND ss.deleted_at IS NULL
            WHERE sr.name = %s
                AND sr.deleted_at IS NULL
                AND ss.year = %s
            ;
        """
    season_results = hook.get_records(select_sql, parameters=(series_name, year))
    # Each result row is a tuple
    for season_result in season_results:
        series_id = int(season_result[0])
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
                INNER JOIN nascar.racing_reference_tracks rrt
                    ON rrt.track_id = t.id
                    AND rrt.deleted_at IS NULL
                WHERE TRIM(rrt.name) = TRIM(%s)
                    AND t.deleted_at IS NULL
                LIMIT 1
                ;
            """
            track_result = hook.get_first(select_sql, parameters=(week_track,))
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
                insert_sql = f"""
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
                hook.run(insert_sql, parameters=(season_id, track_id, week_number, week_race_name, week_date))
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
                record = hook.get_first(sql, parameters=(season_id, track_id, week_number, week_race_name, week_date))
                week_id = record[0]
            # If the week does not have a recorded race
            if week_is_race_completed is False and week_url is not None and week_url != '':
                logging.info(f"No race recorded for {series_name} series {str(year)} week {week_number}")
                logging.info(f"Downloading files and attempting to insert available data...")
                # Follow the week_url
                base_path = source.storage_path_week(series_name, year, week_number)
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
                    user_agent=random.choice(USER_AGENTS)
                )
                race_results_html = f"{base_path}/race-results.html"
                conv.convert_race_results(race_results_html, f"{base_path}/race-results.csv")
                conv.convert_prerace_penalties(race_results_html, f"{base_path}/pre-race-penalties.csv")
                conv.convert_caution_flags(race_results_html, f"{base_path}/caution-flags.csv")
                conv.convert_lap_leaders(race_results_html, f"{base_path}/lap-leaders.csv")
                download_entrylist_html(source, html_file_path=race_results_html, series_name=series_name, year=year, week_number=week_number)
                entrylist_html_path = f"{base_path}/entrylist.html"
                entrylist_csv_path = f"{base_path}/entrylist.csv"
                if Path(entrylist_html_path).is_file():
                    conv.convert_entrylist(entrylist_html_path, entrylist_csv_path)
                practice_html_file_paths = download_practices(source, html_file_path=race_results_html, series_name=series_name, year=year, week_number=week_number)
                conv.convert_practices(practice_html_file_paths)
                qualifying_html_file_path = download_qualifying(source, html_file_path=race_results_html, series_name=series_name, year=year, week_number=week_number)
                conv.convert_qualifying(qualifying_html_file_path)
                loop_data_html_file_path = download_loop_data(source, html_file_path=race_results_html, series_name=series_name, year=year, week_number=week_number)
                conv.convert_loop_data(loop_data_html_file_path)
                penalties_html_file_path = download_penalties(source, html_file_path=race_results_html, series_name=series_name, year=year, week_number=week_number)
                conv.convert_penalties(penalties_html_file_path)

                hook = MySqlTemplateFileExecutor(
                    mysql_conn_id="nascar_conn",
                    parameters={
                        "week_id": week_id
                    }
                )

                if Path(entrylist_csv_path).is_file():
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent / "sql" / "nascar_scraper" / "load_entrylist_data.sql",
                        context={
                            "week_id": week_id,
                            "entrylist_csv_file_path": entrylist_csv_path
                        }
                    )

                practice_files = glob.glob(f"{base_path}/practice-*.csv")
                number_of_practices = len(practice_files)
                for i in range(1, number_of_practices + 1):
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent / "sql" / "nascar_scraper" / "load_practice_data.sql",
                        context={
                            "week_id": week_id,
                            "number": i,
                            "practice_csv_file_path": practice_files[i - 1]
                        }
                    )

                if len(glob.glob(f"{base_path}/qualifying.csv")) > 0:
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent / "sql" / "nascar_scraper" / "load_qualifying_data.sql",
                        context={
                            "week_id": week_id,
                            "qualifying_csv_file_path": f"{base_path}/qualifying.csv"
                        }
                    )

                if len(glob.glob(f"{base_path}/race-results.csv")) > 0:
                    hook.run_templated_sql_file(
                        file_path=Path(__file__).parent / "sql" / "nascar_scraper" / "load_race_data.sql",
                        context={
                            "week_id": week_id,
                            "race_results_csv_file_path": f"{base_path}/race-results.csv",
                            "race_loop_data_csv_file_path": f"{base_path}/loop-data.csv"
                        }
                    )

                hook.run_templated_sql_file(
                    file_path=Path(__file__).parent / "sql" / "nascar_scraper" / "load_clean_up.sql",
                    context={
                        "week_id": week_id,
                    }
                )
                
            else:
                if week_result is not None:
                    total_matches = int(week_result[3])
                    logging.info(f"Number of matching weeks: {str(total_matches)}")
                else:
                    logging.info(f"No new data to load.")

with DAG(
    "nascar_scraper",
    start_date=datetime(2025, 7, 11),
    schedule="0 */4 * * *",
    catchup=True,
    max_active_runs=1
) as dag:

    wait_random_time_task = WaitOperator(
        task_id="wait_random_time",
        seconds=random.randint(0, 5), # 0 to 30 minutes
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
        sql_file_path=Path(__file__).parent / "sql" / "nascar_scraper" / "create_season_if_not_exists.sql",
        mysql_conn_id="nascar_conn",
        parameters={
            "series_name": "Cup"
        },
    )

    cup_series_create_tracks_if_not_exists_task = PythonOperator(
        task_id="cup_series_create_tracks_if_not_exists",
        python_callable=create_tracks_if_not_exists,
        op_kwargs={
            "series_name": "Cup"
        }
    )

    cup_series_create_weeks_if_not_exists_task = PythonOperator(
        task_id="cup_series_create_weeks_if_not_exists",
        python_callable=create_weeks_if_not_exists,
        op_kwargs={
            "series_name": "Cup"
        }
    )

    wait_random_time_task >> cup_series_download_year_schedule_html_task >> cup_series_convert_year_schedule_html_to_csv_task >> cup_series_create_season_if_not_exists_task >> cup_series_create_tracks_if_not_exists_task >> cup_series_create_weeks_if_not_exists_task
