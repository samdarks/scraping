"""Handles The Id filtering"""

from typing import Set
import os
import pandas as pd


def input_data() -> Set[int]:
    """Get the input ids"""
    input_directory = os.listdir("/Users/sam/Documents/scraping/tiktok_scraper/input/")
    print(input_directory, "this is the input directory")
    try:
        in_data: pd.DataFrame = pd.concat(
            [
                pd.read_csv(f"/Users/sam/Documents/scraping/tiktok_scraper/input/{file}")  # type: ignore
                for file in input_directory if file.endswith('.csv') 
            ]
        )
        input_ids = set(in_data["video_id"].values)
        return input_ids
    except ValueError:
        return {0}

def output_data() -> Set[int]:
    """Get the scraped ids"""
    output_directory = os.listdir("/Users/sam/Documents/scraping/tiktok_scraper/output/")
    try:
        already_scraped: pd.DataFrame = pd.concat(
            [
                pd.read_csv(f"/Users/sam/Documents/scraping/tiktok_scraper/output/{file}")  # type: ignore
                for file in output_directory if file.endswith('.csv')
            ]
        )
        already_ids = set(already_scraped["video_id"].values)
        return already_ids
    except ValueError:
        return {0}


def failed_data() -> Set[int]:
    """Get the failed ids"""
    failed_directory = os.listdir("/Users/sam/Documents/scraping/tiktok_scraper/failed/")
    try:
        fail_data: pd.DataFrame = pd.concat(
            [
                pd.read_csv(f"/Users/sam/Documents/scraping/tiktok_scraper/failed/{file}")  # type: ignore
                for file in failed_directory if file.endswith('.csv')
            ]
        )
        failed_ids = set(fail_data["video_id"].values)
        return failed_ids
    except ValueError:
        return {0}


def delayed_data() -> Set[int]:
    """Get the delay ids"""
    delay_directory = os.listdir("/Users/sam/Documents/scraping/tiktok_scraper/delay/")
    try:
        delay_data: pd.DataFrame = pd.concat(
            [
                pd.read_csv(f"/Users/sam/Documents/scraping/tiktok_scraper/delay/{file}")  # type: ignore
                for file in delay_directory if file.endswith('.csv')
            ]
        )
        delay_ids = set(delay_data["video_id"].values)
        return delay_ids
    except ValueError:
        return {0}


failed_and_scraped_ids = output_data() | failed_data()


ids = input_data().difference(failed_and_scraped_ids) | delayed_data()
