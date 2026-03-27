import csv
import logging
import os
import re
import pandas as pd

from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import timedelta, datetime
from bs4 import BeautifulSoup
from include.utils.file import File

class CsvReader:
    def __init__(self, html: str):
        """
        Initialize with raw HTML content as a string.
        """
        self.html = html
        self.soup = None
        self.data = []

    @staticmethod
    def insert_practice_data(csv_file_path):
        rows = File.read_csv(csv_file_path, as_dict=True)
        hook = MySqlHook(mysql_conn_id="nascar_conn")
        sql = f"""
            CREATE TEMPORARY TABLE
        """
        for row in rows:
            results = hook.get_records(select_sql, parameters=(track_name,))
