"""youtube"""

from typing import Iterable, Dict, Any, List

import re
import scrapy
from scrapy.http.request import Request
from scrapy.http.response import Response
import simdjson
from youtube_scraper.id_handler import ids


class YoutubeSpider(scrapy.Spider):
    """class"""

    name = "youtube"
    allowed_domains = ["www.youtube.com"]
    start_urls = ["https://www.youtube.com/watch?v="]

    def start_requests(self) -> Iterable[Request]:
        """start"""
        for tid in ids:

            url = f"{self.start_urls[0]}{tid}&rco=1"
            yield scrapy.Request(
                url=url,
                # cookies=cookies,
                callback=self.parse,
                meta={
                    "tid": tid,
                },
            )

    def parse(self, response: Response) -> Iterable[Dict[str, Any]]:
        """parser"""
        script_text = response.css("body > script:nth-child(2)::text").get("")

        video_info = (
            self.parse_script_text(script_text)
            .get("microformat", {})
            .get("playerMicroformatRenderer")
        )

        if video_info:

            hashtags = self.extract_hashtags(
                video_info.get("title").get("simpleText", "")
            )

            yield {
                "video_id": response.meta.get("tid"),
                "Video length": video_info.get("lengthSeconds"),
                "Video Category": video_info.get("category"),
                "Creator Profile": video_info.get("ownerProfileUrl"),
                "Channel": video_info.get("ownerChannelName"),
                "likes": video_info.get("likeCount"),
                "Views": video_info.get("viewCount"),
                "Hashtag": hashtags,
            }

    def parse_script_text(self, script_text: str) -> Dict[str, Any]:
        """formatter"""
        pattern = r"var\s+ytInitialPlayerResponse\s*=\s*(\{.*?\});"

        match = re.search(pattern, script_text, re.DOTALL)
        if match:
            matched_text = match.group(1)
            return simdjson.loads(matched_text)
        return {"": ""}

    def extract_hashtags(self, description: str) -> List[str]:
        """extractor"""

        matches = re.findall(r"#\s*([^\n#]+)", description)

        if matches:
            return matches

        return []


# {
#     "date": datetime.datetime.now().strftime("%Y/%m/%d"),
#     "Time": datetime.datetime.now().strftime("%H:%M:%S"),
#     "title": video_info.get("title").get("simpleText"),
#     "Event": "watched",
#     "Creator Profile": video_info.get("ownerProfileUrl"),
#     "Channel": video_info.get("ownerChannelName"),
#     "url": video_info.get("canonicalUrl"),
#     "device": other_info.get("responseContext", {})
#     .get("serviceTrackingParams")[1]
#     .get("params")[0]
#     .get("value"),
#     "video_id": response.meta.get("tid"),
#     "likes": video_info.get("likeCount"),
#     "Views": video_info.get("viewCount"),
#     "Video Category": video_info.get("category"),
#     "posted Date": video_info.get("publishDate"),
#     "Video length": video_info.get("lengthSeconds"),
#     "Hashtag": hashtags,
# }
