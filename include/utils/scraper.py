import logging
import os
import time

from .file import File
from .utils import Utils

_log = logging.getLogger(__name__)


def _url_or_filename_is_pdf(url: str, filename: str) -> bool:
    if filename.lower().endswith(".pdf"):
        return True
    u = url.lower().split("?", 1)[0].split("#", 1)[0]
    return u.endswith(".pdf")


class Scraper(Utils):
    def __init__(self, output_folder: str):
        super().__init__()
        self.output_folder = output_folder
        os.makedirs(output_folder, exist_ok=True)

    def _download_binary_via_request(
        self, url: str, filename: str, user_agent: str, max_attempts: int
    ) -> None:
        """Fetch raw bytes (e.g. PDF) — ``page.goto`` often aborts or returns wrapper HTML."""
        from playwright.sync_api import sync_playwright

        attempt = 0
        last_exception = None
        filepath = os.path.join(self.output_folder, filename)

        while attempt < max_attempts:
            try:
                with sync_playwright() as p:
                    req = p.request.new_context(user_agent=user_agent)
                    try:
                        resp = req.get(url, timeout=60_000)
                        if not resp.ok:
                            raise RuntimeError(f"HTTP {resp.status} {resp.status_text}")
                        body = resp.body()
                    finally:
                        req.dispose()
                if len(body) < 100 or not body.startswith(b"%PDF"):
                    raise RuntimeError(
                        f"Response is not a PDF (got {len(body)} bytes, start={body[:20]!r})"
                    )
                os.makedirs(self.output_folder, exist_ok=True)
                with open(filepath, "wb") as f:
                    f.write(body)
                return
            except Exception as e:
                attempt += 1
                last_exception = e
                if attempt < max_attempts:
                    _log.debug(
                        "Binary download attempt %s/%s failed for %s: %s",
                        attempt,
                        max_attempts,
                        filename,
                        e,
                    )
                time.sleep(1)

        _log.error(
            "Binary download failed after %s attempts (%s -> %s): %s",
            max_attempts,
            url,
            filepath,
            last_exception,
        )
        raise last_exception

    def download(self, url, filename, user_agent, max_attempts=3):
        """
        Download file using playwright. HTML pages use a headed browser; PDFs use HTTP GET
        so navigation is not aborted by the PDF viewer.
        """

        if _url_or_filename_is_pdf(url, filename):
            return self._download_binary_via_request(url, filename, user_agent, max_attempts)

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
                if attempt < max_attempts:
                    _log.debug(
                        "Download attempt %s/%s failed for %s: %s",
                        attempt,
                        max_attempts,
                        filename,
                        e,
                    )
                time.sleep(1)  # optional backoff between retries

        _log.error(
            "Download failed after %s attempts (%s -> %s): %s",
            max_attempts,
            url,
            os.path.join(self.output_folder, filename),
            last_exception,
        )
        raise last_exception
