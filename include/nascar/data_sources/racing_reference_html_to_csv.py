"""HTML → CSV parsers for www.racing-reference.info markup."""

from __future__ import annotations

import csv
import logging
import os
import re
from datetime import datetime

from bs4 import BeautifulSoup

from include.utils.file import File

# ``td`` race rows: class may be ``col``, ``col odd``, or (on some saves) omitted.
_RR_TD_COL_CLASS = re.compile(r"(^|\s)col(\s|$)")


class RacingReferenceHtmlToCsv:
    """Parsers for Racing Reference result/schedule tables (used by load SQL)."""

    def __init__(self, html: str):
        self.html = html
        self.soup = None
        self.data: list = []

    def parse_html(self) -> None:
        self.soup = BeautifulSoup(self.html, "html.parser")

    def save_to_csv(self, filename: str, header: list[str] | None = None) -> None:
        try:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, lineterminator="\n")
                if header:
                    writer.writerow(header)
                writer.writerows(self.data)
        except Exception as e:
            raise RuntimeError(f"Failed to write CSV file {filename}: {e}") from e

    @staticmethod
    def convert_schedule(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        rows = soup.find_all("div", {"class": "table-row"})
        csv_list: list = []
        csv_row: dict[str, str] = {}
        for row in rows:
            raceweek_number_div = row.find("div", {"class": "race-number"})
            raceweek_number = raceweek_number_div.get_text()
            raceweek_url = ""
            if raceweek_number_div.find("a"):
                raceweek_url = raceweek_number_div.find("a").get("href")
            raceweek_title = ""
            if raceweek_number_div.find("a"):
                raceweek_title = raceweek_number_div.find("a").get("title")
            else:
                upcoming = row.find("div", {"class": "upcoming"})
                raceweek_title = upcoming.get_text().strip() if upcoming else ""
                raceweek_title = re.findall('"([^"]*)"', raceweek_title)
                raceweek_title = raceweek_title[0] if len(raceweek_title) else ""
            raceweek_date = row.find("div", {"class": "date"}).get_text()
            raceweek_track = row.find("div", {"class": "track"}).find("a").get_text()
            raceweek_track_url = row.find("div", {"class": "track"}).find("a")["href"]
            csv_row = {
                "week_number": raceweek_number.strip(),
                "week_url": raceweek_url.strip(),
                "week_date": datetime.strptime(raceweek_date.strip(), "%m/%d/%y").strftime(
                    "%Y-%m-%d"
                ),
                "week_track": raceweek_track.strip(),
                "week_track_url": raceweek_track_url.strip(),
                "week_race_name": raceweek_title.strip(),
            }
            csv_list.append(csv_row.values())
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, csv_row.keys())
        logging.info("Racing Reference: schedule %d rows -> %s", len(csv_list), csv_file_path)

    @staticmethod
    def _race_results_row_cells(tr) -> list:
        """Collect result ``td``/``th`` cells; RR sometimes omits ``class=\"col\"`` on saved HTML."""
        cols = tr.find_all("td", class_=_RR_TD_COL_CLASS)
        if len(cols) >= 10:
            return cols
        direct = tr.find_all(["td", "th"], recursive=False)
        if len(direct) >= 10:
            return direct
        return tr.find_all(["td", "th"])

    @staticmethod
    def _race_results_row_to_fields(cells: list) -> dict[str, str] | None:
        """
        Map table cells to CSV columns expected by ``load_race_data.sql``.

        Layouts:
        - 11 cells: no Money column (Cup-style).
        - 12 cells: Money between Laps and Status (many RR tables).
        - 10 cells: no Playoff points column.
        """
        n = len(cells)
        if n < 10:
            return None

        def txt(i: int) -> str:
            return cells[i].get_text(strip=True) if i < n else ""

        sponsor_cell = cells[4]
        sponsor = (
            sponsor_cell.find("b").get_text(strip=True) if sponsor_cell.find("b") else ""
        )
        owner = (
            sponsor_cell.find("a").get_text(strip=True) if sponsor_cell.find("a") else ""
        )

        first = txt(0).lower()
        if first in ("pos", "fin", "pst", "#", "st") or first.startswith("pos"):
            return None

        if n == 10:
            return {
                "position": txt(0),
                "starting_position": txt(1),
                "number": txt(2),
                "name": txt(3),
                "sponsor": sponsor,
                "owner": owner,
                "car": txt(5),
                "laps": txt(6),
                "status": txt(7),
                "laps_led": txt(8),
                "points": txt(9),
                "playoff_points": "",
            }
        if n == 11:
            return {
                "position": txt(0),
                "starting_position": txt(1),
                "number": txt(2),
                "name": txt(3),
                "sponsor": sponsor,
                "owner": owner,
                "car": txt(5),
                "laps": txt(6),
                "status": txt(7),
                "laps_led": txt(8),
                "points": txt(9),
                "playoff_points": txt(10),
            }
        # 12+ : Money (or similar) at index 7 after laps
        return {
            "position": txt(0),
            "starting_position": txt(1),
            "number": txt(2),
            "name": txt(3),
            "sponsor": sponsor,
            "owner": owner,
            "car": txt(5),
            "laps": txt(6),
            "status": txt(8),
            "laps_led": txt(9),
            "points": txt(10),
            "playoff_points": txt(11) if n > 11 else "",
        }

    @staticmethod
    def convert_race_results(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        results_table = soup.find("table", {"class": "race-results-tbl"})
        csv_list: list = []
        headers = [
            "position",
            "starting_position",
            "number",
            "name",
            "sponsor",
            "owner",
            "car",
            "laps",
            "status",
            "laps_led",
            "points",
            "playoff_points",
        ]
        if results_table is not None:
            tbody = results_table.find("tbody")
            row_container = tbody or results_table
            for row in row_container.find_all("tr"):
                if row.find("td") is None and row.find("th") is not None:
                    continue
                cells = RacingReferenceHtmlToCsv._race_results_row_cells(row)
                fields = RacingReferenceHtmlToCsv._race_results_row_to_fields(cells)
                if fields is None:
                    continue
                csv_list.append(fields.values())
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info("Racing Reference: race results %d rows -> %s", len(csv_list), csv_file_path)

    @staticmethod
    def convert_prerace_penalties(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        or_cell = soup.find("td", {"class": "otherResultsTbl"})
        other_results_tables = (
            or_cell.find_all("table", {"class": "tb"}) if or_cell is not None else []
        )
        tableIndex = None
        index = 0
        csv_list: list = []
        headers = ["name", "number", "reason"]
        for table in other_results_tables:
            newhead = table.find("td", {"class": "newhead"})
            table_heading = newhead.get_text().strip() if newhead else ""
            if table_heading == "To the back:":
                tableIndex = index
                break
            index += 1
        if tableIndex is not None:
            table = other_results_tables[tableIndex]
            rows = table.find_all("tr")[2:]
            for row in rows:
                cols = row.find_all("td", {"class": "col"})
                if len(cols) < 3:
                    continue
                name = cols[0].get_text().strip()
                number = cols[1].get_text().strip()
                reason = cols[2].get_text().strip()
                csv_list.append({"name": name, "number": number, "reason": reason}.values())
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info(
            "Racing Reference: pre-race penalties %d rows -> %s",
            len(csv_list),
            csv_file_path,
        )

    @staticmethod
    def convert_caution_flags(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        or_cell = soup.find("td", {"class": "otherResultsTbl"})
        other_results_tables = (
            or_cell.find_all("table", {"class": "tb"}) if or_cell is not None else []
        )
        tableIndex = None
        index = 0
        csv_list: list = []
        headers = ["condition", "from_lap", "to_lap", "number_of_laps", "reason", "free_pass_cars"]
        for table in other_results_tables:
            newhead = table.find("td", {"class": "newhead"})
            table_heading = newhead.get_text().strip() if newhead else ""
            if table_heading == "Caution flag breakdown:":
                tableIndex = index
                break
            index += 1
        if tableIndex is not None:
            table = other_results_tables[tableIndex]
            rows = table.find_all("tr")[2:]
            for row in rows:
                cols = row.find_all("td", {"class": "col"})
                if len(cols) < 6:
                    continue
                img_tag = cols[0].find("img")
                src = img_tag.get("src") if img_tag is not None else None
                if src:
                    condition = os.path.splitext(os.path.basename(src))[0].replace("_flag", "")
                else:
                    condition = cols[0].get_text().strip()
                from_lap = cols[1].get_text().strip()
                to_lap = cols[2].get_text().strip()
                number_of_laps = cols[3].get_text().strip()
                reason = cols[4].get_text().strip()
                free_pass_cars = cols[5].get_text().strip().replace("#", "")
                csv_list.append(
                    {
                        "condition": condition,
                        "from_lap": from_lap,
                        "to_lap": to_lap,
                        "number_of_laps": number_of_laps,
                        "reason": reason,
                        "free_pass_cars": free_pass_cars,
                    }.values()
                )
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info(
            "Racing Reference: caution flags %d rows -> %s",
            len(csv_list),
            csv_file_path,
        )

    @staticmethod
    def convert_lap_leaders(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        or_cell = soup.find("td", {"class": "otherResultsTbl"})
        other_results_tables = (
            or_cell.find_all("table", {"class": "tb"}) if or_cell is not None else []
        )
        tableIndex = None
        index = 0
        csv_list: list = []
        headers = ["name", "from_lap", "to_lap", "number_of_laps"]
        for table in other_results_tables:
            newhead = table.find("td", {"class": "newhead"})
            table_heading = newhead.get_text().strip() if newhead else ""
            if table_heading == "Lap leader breakdown:":
                tableIndex = index
                break
            index += 1
        if tableIndex is not None:
            table = other_results_tables[tableIndex]
            rows = table.find_all("tr")[2:]
            for row in rows:
                cols = row.find_all("td", {"class": "col"})
                if len(cols) < 4:
                    continue
                name = cols[0].get_text().strip()
                from_lap = cols[1].get_text().strip()
                to_lap = cols[2].get_text().strip()
                number_of_laps = cols[3].get_text().strip()
                csv_list.append(
                    {
                        "name": name,
                        "from_lap": from_lap,
                        "to_lap": to_lap,
                        "number_of_laps": number_of_laps,
                    }.values()
                )
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info(
            "Racing Reference: lap leaders %d rows -> %s",
            len(csv_list),
            csv_file_path,
        )

    @staticmethod
    def convert_entrylist(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        tables = soup.find("table", {"class": "eLTbl"}).find_all("table", {"class": "tb"})
        tableIndex = None
        index = 0
        csv_list: list = []
        headers = ["number", "driver_name", "owner", "manufacturer", "crew_chief", "sponsor"]
        for table in tables:
            table_heading = table.find("td", {"class": "newhead"}).get_text().strip()
            if table_heading == "Entry list for this race:":
                tableIndex = index
                break
            index += 1
        if tableIndex is not None:
            table = tables[tableIndex]
            rows = table.find_all("tr", class_=["odd", "even"])
            for row in rows:
                cols = row.find_all("td", {"class": "col"})
                if cols:
                    number = cols[1].get_text().strip()
                    driver_name = (
                        cols[2].find("a").get_text().strip()
                        if cols[2].find("a") is not None
                        else ""
                    )
                    owner = cols[3].get_text().strip()
                    manufacturer = cols[4].get_text().strip()
                    crew_chief = cols[5].get_text().strip()
                    sponsor = cols[6].get_text().strip()
                    csv_list.append(
                        {
                            "number": number,
                            "driver_name": driver_name,
                            "owner": owner,
                            "manufacturer": manufacturer,
                            "crew_chief": crew_chief,
                            "sponsor": sponsor,
                        }.values()
                    )
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info("Racing Reference: entry list %d rows -> %s", len(csv_list), csv_file_path)

    @staticmethod
    def convert_practices(paths: list[str]) -> None:
        for path in paths:
            with open(path, encoding="utf-8") as html_file:
                soup = BeautifulSoup(html_file, "html.parser")
            tables = soup.find_all("table", {"class": "pracResultsTbl"})
            tableIndex = None
            index = 0
            csv_list: list = []
            headers = [
                "rank",
                "driver_name",
                "number",
                "manufacturer",
                "time",
                "speed",
                "num_laps",
                "best_lap",
            ]
            for table in tables:
                nh = table.find("td", {"class": "newhead"})
                table_heading = nh.get_text().strip().lower() if nh else ""
                logging.debug("Racing Reference: practice table_heading=%r path=%s", table_heading, path)
                if "practice" in table_heading and "results" in table_heading:
                    tableIndex = index
                    break
                index += 1
            if tableIndex is not None:
                table = tables[tableIndex]
                rows = table.find_all("tr", class_=["odd", "even"])
                for row in rows:
                    cols = row.find_all("td", {"class": "col"})
                    if not cols:
                        continue
                    # Cup practice table (``pracResultsTbl``), 9 data cells:
                    # 0 Pos, 1 Driver, 2 #, 3 Mfr, 4 lap time, 5 sec behind leader (omit),
                    # 6 speed (MPH), 7 laps, 8 best lap. Leader row often has empty col 5.
                    if len(cols) < 8:
                        continue
                    rank = cols[0].get_text().strip()
                    driver_name = (
                        cols[1].find("a").get_text().strip()
                        if cols[1].find("a") is not None
                        else cols[1].get_text().strip()
                    )
                    number = cols[2].get_text().strip()
                    manufacturer = cols[3].get_text().strip()
                    time = RacingReferenceHtmlToCsv.convert_to_seconds(cols[4].get_text().strip())
                    if len(cols) >= 9:
                        speed = cols[6].get_text().strip()
                        num_laps = cols[7].get_text().strip()
                        best_lap = cols[8].get_text().strip()
                    else:
                        speed = cols[5].get_text().strip()
                        num_laps = cols[6].get_text().strip()
                        best_lap = cols[7].get_text().strip()
                    csv_list.append(
                        {
                            "rank": rank,
                            "driver_name": driver_name,
                            "number": number,
                            "manufacturer": manufacturer,
                            "time": time,
                            "speed": speed,
                            "num_laps": num_laps,
                            "best_lap": best_lap,
                        }.values()
                    )
            csv_file_path = path.replace(".html", ".csv")
            File.silent_remove(csv_file_path)
            File.write_csv(csv_file_path, csv_list, headers)
            logging.info(
                "Racing Reference: practice %d rows %s -> %s",
                len(csv_list),
                path,
                csv_file_path,
            )

    @staticmethod
    def convert_qualifying(path: str | None) -> None:
        if path is None:
            logging.info("Racing Reference: qualifying skipped (no HTML path)")
            return
        with open(path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        tables = soup.find_all("table", {"class": "qualResultTbl"})
        tableIndex = None
        index = 0
        csv_list: list = []
        headers = ["rank", "driver_name", "number", "manufacturer", "time", "speed"]
        for table in tables:
            table_heading = table.find("td", {"class": "newhead"}).get_text().strip().lower()
            logging.debug(
                "Racing Reference: qualifying table_heading=%r path=%s",
                table_heading,
                path,
            )
            if "qualifying" in table_heading and "results" in table_heading:
                tableIndex = index
                break
            index += 1
        if tableIndex is not None:
            table = tables[tableIndex]
            rows = table.find_all("tr", class_=["odd", "even"])
            for row in rows:
                cols = row.find_all("td", {"class": "col"})
                if cols:
                    rank = cols[0].get_text().strip()
                    driver_name = (
                        cols[1].find("a").get_text().strip()
                        if cols[1].find("a") is not None
                        else ""
                    )
                    number = cols[2].get_text().strip()
                    manufacturer = cols[3].get_text().strip()
                    time = RacingReferenceHtmlToCsv.convert_to_seconds(cols[4].get_text().strip())
                    speed = cols[5].get_text().strip()
                    csv_list.append(
                        {
                            "rank": rank,
                            "driver_name": driver_name,
                            "number": number,
                            "manufacturer": manufacturer,
                            "time": time,
                            "speed": speed,
                        }.values()
                    )
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info(
            "Racing Reference: qualifying %d rows %s -> %s",
            len(csv_list),
            path,
            csv_file_path,
        )

    @staticmethod
    def convert_loop_data(path: str | None) -> None:
        if path is None:
            logging.info("Racing Reference: loop data skipped (no HTML path)")
            return
        with open(path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        tables = soup.find_all("table", {"class": "loopData"})
        tableIndex = None
        index = 0
        csv_list: list = []
        headers = [
            "driver_name",
            "start",
            "mid_race",
            "finish",
            "high_pos",
            "low_pos",
            "avg_pos",
            "pass_diff",
            "green_flag_passes",
            "green_flag_times_passed",
            "quality_passes",
            "pct_quality_passes",
            "fastest_lap",
            "top_15_laps",
            "pct_top_15_laps",
            "laps_led",
            "pct_laps_led",
            "total_laps",
            "driver_rating",
        ]
        for table in tables:
            table_heading = table.find("td", {"class": "newhead"}).get_text().strip().lower()
            logging.debug(
                "Racing Reference: loop data table_heading=%r path=%s",
                table_heading,
                path,
            )
            if "loop" in table_heading and "data" in table_heading:
                tableIndex = index
                break
            index += 1
        if tableIndex is not None:
            table = tables[tableIndex]
            rows = table.find_all("tr", class_=["odd", "even"])
            for row in rows:
                cols = row.find_all("td", {"class": "col"})
                if cols:
                    driver_name = (
                        cols[0].find("a").get_text().strip()
                        if cols[0].find("a") is not None
                        else ""
                    )
                    start = cols[1].get_text().strip()
                    mid_race = cols[2].get_text().strip()
                    finish = cols[3].get_text().strip()
                    high_pos = cols[4].get_text().strip()
                    low_pos = cols[5].get_text().strip()
                    avg_pos = cols[6].get_text().strip()
                    pass_diff = cols[7].get_text().strip()
                    green_flag_passes = cols[8].get_text().strip()
                    green_flag_times_passed = cols[9].get_text().strip()
                    quality_passes = cols[10].get_text().strip()
                    pct_quality_passes = cols[11].get_text().strip()
                    fastest_lap = cols[12].get_text().strip()
                    top_15_laps = cols[13].get_text().strip()
                    pct_top_15_laps = cols[14].get_text().strip()
                    laps_led = cols[15].get_text().strip()
                    pct_laps_led = cols[16].get_text().strip()
                    total_laps = cols[17].get_text().strip()
                    driver_rating = cols[18].get_text().strip()
                    csv_list.append(
                        {
                            "driver_name": driver_name,
                            "start": start,
                            "mid_race": mid_race,
                            "finish": finish,
                            "high_pos": high_pos,
                            "low_pos": low_pos,
                            "avg_pos": avg_pos,
                            "pass_diff": pass_diff,
                            "green_flag_passes": green_flag_passes,
                            "green_flag_times_passed": green_flag_times_passed,
                            "quality_passes": quality_passes,
                            "pct_quality_passes": pct_quality_passes,
                            "fastest_lap": fastest_lap,
                            "top_15_laps": top_15_laps,
                            "pct_top_15_laps": pct_top_15_laps,
                            "laps_led": laps_led,
                            "pct_laps_led": pct_laps_led,
                            "total_laps": total_laps,
                            "driver_rating": driver_rating,
                        }.values()
                    )
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info(
            "Racing Reference: loop data %d rows %s -> %s",
            len(csv_list),
            path,
            csv_file_path,
        )

    @staticmethod
    def convert_penalties(path: str | None) -> None:
        if path is None:
            logging.info("Racing Reference: penalties skipped (no HTML path)")
            return
        with open(path, encoding="utf-8") as html_file:
            soup = BeautifulSoup(html_file, "html.parser")
        tables = soup.find_all("table", {"class": "penaltTbl"})
        tableIndex = None
        index = 0
        csv_list: list = []
        headers = ["number", "driver_name", "lap", "flag", "infraction", "penalty"]
        for table in tables:
            table_heading = table.find("td", {"class": "newhead"}).get_text().strip().lower()
            logging.debug(
                "Racing Reference: penalties table_heading=%r path=%s",
                table_heading,
                path,
            )
            if "penalty" in table_heading:
                tableIndex = index
                break
            index += 1
        if tableIndex is not None:
            table = tables[tableIndex]
            rows = table.find_all("tr", class_=["odd", "even"])[1:]
            for row in rows:
                cols = row.find_all("td", {"class": "col"})
                if cols:
                    number = cols[0].get_text().strip()
                    driver_name = (
                        cols[1].find("a").get_text().strip()
                        if cols[1].find("a") is not None
                        else ""
                    )
                    lap = cols[2].get_text().strip()
                    flag = os.path.splitext(os.path.basename(cols[3].find("img").get("src")))[
                        0
                    ].replace("_flag", "")
                    infraction = cols[4].get_text().strip()
                    penalty = cols[5].get_text().strip()
                    csv_list.append(
                        {
                            "number": number,
                            "driver_name": driver_name,
                            "lap": lap,
                            "flag": flag,
                            "infraction": infraction,
                            "penalty": penalty,
                        }.values()
                    )
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)
        logging.info(
            "Racing Reference: penalties %d rows %s -> %s",
            len(csv_list),
            path,
            csv_file_path,
        )

    @staticmethod
    def convert_to_seconds(time_str: str) -> float:
        if not time_str:
            return 0.0

        if ":" in time_str:
            minutes, seconds = time_str.split(":")
            return int(minutes) * 60 + float(seconds)
        return float(time_str)
