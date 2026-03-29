import os
import time

from .file import File
from .utils import Utils


class Scraper(Utils):
    def __init__(self, output_folder: str):
        super().__init__()
        self.output_folder = output_folder
        os.makedirs(output_folder, exist_ok=True)

    def download(self, url, filename, user_agent, max_attempts=3):
        """
        Download file using playwrite to simulate a browser
        """

        from playwright.sync_api import sync_playwright

        attempt = 0
        last_exception = None

        while attempt < max_attempts:
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
                    context = browser.new_context(user_agent=user_agent)
                    page = context.new_page()
                    page.goto(url, timeout=60000)
                    page.wait_for_load_state("load")
                    html = page.content()
                    browser.close()

                # Save file if successful
                filepath = os.path.join(self.output_folder, filename)
                File.write(filename=filepath, content=html)

                # Success: break the loop
                return

            except Exception as e:
                attempt += 1
                last_exception = e
                print(f"Attempt {attempt} failed: {e}")
                time.sleep(1)  # optional backoff between retries

        # If we reach here, all attempts failed
        raise last_exception
