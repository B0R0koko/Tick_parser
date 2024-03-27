from scrapy import Spider
from typing import *

import os


class ZipPipeline:
    def __init__(self, output_dir: str):
        self.output_dir: str = output_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            output_dir=crawler.settings.get("OUTPUT_DIR"),
        )

    def process_item(self, response: Dict[str, Any], spider: Spider):
        ticker, slug, data = response["ticker"], response["slug"], response["data"].body
        ticker_dir = os.path.join(self.output_dir, ticker)

        os.makedirs(ticker_dir, exist_ok=True)

        with open(os.path.join(ticker_dir, f"{slug}.zip"), "wb") as file:
            file.write(data)
