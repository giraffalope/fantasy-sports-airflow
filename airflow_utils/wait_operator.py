import time
from airflow.sdk import BaseOperator

class WaitOperator(BaseOperator):
    """
    Custom operator that waits for a given number of seconds.
    """

    def __init__(self, seconds: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.seconds = seconds

    def execute(self, context):
        self.log.info(f"Waiting for {self.seconds} seconds...")
        time.sleep(self.seconds)
        self.log.info("Done waiting.")
