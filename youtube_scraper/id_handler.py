"""Handles The Id filtering"""

from typing import Set
import os
import pandas as pd

print("\n\n TEsting \n\n")


def input_data() -> Set[int]:
    """Get the input ids"""
    input_directory = os.listdir(
        "/Users/sam/Documents/scraping/youtube_scraper/input/"
    )
    try:
        in_data: pd.DataFrame = pd.concat(
            [
                pd.read_parquet(  # type: ignore
                    f"/Users/sam/Documents/scraping/youtube_scraper/input/{file}"
                )
                for file in input_directory if file.endswith('.parquet') 
            ]
        )
        input_ids = set(in_data["video_id"].values)
        print(f"\n\n {input_directory} \n\n")
        return input_ids
    except ValueError:
        return {0}



def output_data() -> Set[int]:
    """Get the scraped ids"""
    output_directory = os.listdir(
        "/Users/sam/Documents/scraping/youtube_scraper/output/"
    )
    try:
        already_scraped: pd.DataFrame = pd.concat(
            [
                pd.read_csv(  # type: ignore
                    f"/Users/sam/Documents/scraping/youtube_scraper/output/{file}"
                )
                for file in output_directory if file.endswith('.csv')
            ]
        )
        already_ids = set(already_scraped["video_id"].values)
        return already_ids
    except ValueError:
        return {0}


# def failed_data() -> Set[int]:
#     """Get the failed ids"""
#     failed_directory = os.listdir(
#         "C:/Users/ahiab/contractal_works/youtube_scraper/failed/"
#     )
#     try:
#         fail_data: pd.DataFrame = pd.concat(
#             [
#                 pd.read_parquet(  # type: ignore
#                     f"C:/Users/ahiab/contractal_works/youtube_scraper/failed/{file}"
#                 )
#                 for file in failed_directory
#             ]
#         )
#         failed_ids = set(fail_data["video_id"].values)
#         return failed_ids
#     except ValueError:
#         return {0}


# def delayed_data() -> Set[int]:
#     """Get the delay ids"""
#     delay_directory = os.listdir(
#         "C:/Users/ahiab/contractal_works/youtube_scraper/delayed/"
#     )
#     try:
#         delay_data: pd.DataFrame = pd.concat(
#             [
#                 pd.read_parquet(  # type: ignore
#                     f"C:/Users/ahiab/contractal_works/youtube_scraper/delayed/{file}"
#                 )
#                 for file in delay_directory
#             ]
#         )
#         delay_ids = set(delay_data["video_id"].values)
#         return delay_ids
#     except ValueError:
#         return {0}


scraped_ids = output_data()


ids = input_data().difference(scraped_ids)


# print(ids)

# ids = [
#     "Om1K6R2iY9c",
#     "VwcFDH1pOnY",
#     "b2R4uZUC5HM",
#     "m9n2xRk76IM",
#     "odZiGSyhdJI",
#     "pktz1WdhNbY",
# ]
