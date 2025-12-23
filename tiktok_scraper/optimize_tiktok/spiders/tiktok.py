"""An optimized Tiktok Scraper"""

from typing import Iterable, Dict, Any, List
import csv
from datetime import datetime
import re
import scrapy
from scrapy.http.request import Request
from scrapy.http.response import Response 
import simdjson
from optimize_tiktok.cookies import cookies
from optimize_tiktok.id_handler import ids


class TiktokSpider(scrapy.Spider):
    """Spider Object"""

    name = "tiktok"
    allowed_domains = ["www.tiktok.com"]
    start_urls = ["https://www.tiktok.com/@tacobell/video/"]
    expired_id: List[str] = []

    def start_requests(self) -> Iterable[Request]:
        for tid in ids:
            url = f"{self.start_urls[0]}{tid}"
            yield scrapy.Request(
                url=url,
                cookies=cookies,
                callback=self.parse,
                meta={
                    "tid": tid,
                },
            )

    def parse(self, response: Response) -> Iterable[Dict[str, Any]]:
        """Data parser"""

        script_text = (
            response.css("#__UNIVERSAL_DATA_FOR_REHYDRATION__::text").get() or ""
        )
        try:
            video_info = (
                simdjson.loads(script_text)
                .get("__DEFAULT_SCOPE__")
                .get("webapp.video-detail")
            )

            if video_info.get("statusMsg") == "" or video_info.get(
                "statusMsg"
            ).startswith("RPCError{PSM:[ies.item"):
                data = video_info.get("itemInfo").get("itemStruct")

                hashtags = [
                    hashtag.get("hashtagName") for hashtag in data.get("textExtra")
                ]

                yield {
                    "url": f"https://www.tiktok.com/@{data.get("author").get("uniqueId") if not re.search(r"[ ,']", data.get("author").get("uniqueId")) else data.get("author").get("secUid")}/video/{response.meta.get("tid")}",
                    "video_id": response.meta.get("tid"),
                    "likes": data.get("statsV2").get("diggCount"),
                    "comment": data.get("statsV2").get("commentCount"),
                    "save": data.get("statsV2").get("collectCount"),
                    "share": data.get("statsV2").get("shareCount"),
                    "creator_username": data.get("author").get("uniqueId"),
                    "creator_display_name": data.get("author").get("nickname"),
                    "posted_date": datetime.fromtimestamp(
                        int(data.get("createTime"))
                    ).strftime("%d-%m-%Y %H:%M:%S"),
                    "sound": data.get("music").get("title"),
                    "description": data.get("desc"),
                    "likes_str": self.string_format(
                        int(data.get("statsV2").get("diggCount"))
                    ),
                    "comment_str": self.string_format(
                        int(data.get("statsV2").get("commentCount"))
                    ),
                    "save_str": self.string_format(
                        int(data.get("statsV2").get("collectCount"))
                    ),
                    "share_str": self.string_format(
                        int(data.get("statsV2").get("shareCount"))
                    ),
                    "input_type": "video_id",
                    "input_value": response.meta.get("tid"),
                    "hashtags": [hashtags],
                    "sponsored": self.is_spornsored(
                        data.get("adLabelVersion"), data.get("isECVideo")
                    ),
                    "author_is_ad_virtual": data.get("author").get("isADVirtual"),
                    "video_data_is_ad": self.is_ad(
                        data.get("adAuthorization"),
                        data.get("isAd"),
                        data.get("isECVideo"),
                    ),
                    "keyword_sponsored": self.keyword_search(
                        ["sponsor", "sponsored"], data.get("desc"), hashtags
                    ),
                    "keyword_ad": self.keyword_search(
                        ["ad"], data.get("desc"), hashtags
                    ),
                    "keyword_promotional": self.keyword_search(
                        ["promotional"], data.get("desc"), hashtags
                    ),
                    "keyword_paid": self.keyword_search(
                        ["paid"], data.get("desc"), hashtags
                    ),
                    "keyword_partnership": self.keyword_search(
                        ["partnership", "partnerships", "paidpartnership"],
                        data.get("desc"),
                        hashtags,
                    ),
                    "promotional Content": self.is_promotional(
                        data.get("brandOrganicType")
                    ),
                }
            else:

                with open(
                    "failed/expired_private_ids.csv",
                    "a",
                    encoding="utf-8",
                    newline="",
                ) as file:
                    writer = csv.writer(file)
                    writer.writerow(
                        [response.meta.get("tid"), video_info.get("statusMsg")]
                    )

        except ValueError:
            with open(
                "delay/server_delayed_id.csv", "a", encoding="utf-8", newline=""
            ) as file:
                writer = csv.writer(file)
                writer.writerow([response.meta.get("tid")])

    def string_format(self, value: int) -> str:
        """stat string formatter"""
        if value < 10_000:
            return str(value)
        if value < 1_000_000:
            return f"{value / 1_000:.1f}K"
        if value < 10_000_000:
            return f"{value / 1_000_000:.1f}M".rstrip("0").rstrip(".")
        return f"{value / 1_000_000:.1f}M"

    def is_spornsored(self, adlabel: int, iecvideo: str) -> bool:
        "paid partnership"
        return adlabel == 2 and not bool(iecvideo)

    def is_ad(self, adauth: bool, ad: bool, iecvideo: str) -> bool:
        """get true or false"""

        return adauth is True or ad or bool(iecvideo)

    def is_promotional(self, organic_brand: str) -> bool:
        "promotional content"
        return organic_brand == "1001"

    def keyword_search(
        self, keywords: List[str], description: str, hashtags: List[str]
    ) -> bool:
        """keyword search"""

        for keyword in keywords:
            pattern = r"\b" + re.escape(keyword) + r"\b"

            if re.search(pattern, description, flags=re.IGNORECASE):
                return True

            for tag in hashtags:
                if re.search(pattern, tag, flags=re.IGNORECASE):
                    return True

        return False
