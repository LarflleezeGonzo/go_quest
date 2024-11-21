import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pydantic import BaseModel, Field, HttpUrl
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)

UTC_Z = "+00:00"


class Video(BaseModel):
    video_id: str = Field(..., min_length=11, max_length=11)
    title: str
    description: Optional[str]
    published_date: datetime
    view_count: int = Field(ge=0)
    like_count: int = Field(ge=0)
    comment_count: int = Field(ge=0)
    duration: str
    thumbnail_url: str


class Comment(BaseModel):
    video_id: str = Field(..., min_length=11, max_length=11)
    comment_id: str
    text: str
    author: str
    published_date: datetime
    like_count: int = Field(ge=0)
    reply_to: Optional[str]


class YouTubeChannelInput(BaseModel):
    url: HttpUrl
    handle: str = Field(..., min_length=1)


ONE_MINUTE = 60
MAX_REQUESTS_PER_MINUTE = 60

@sleep_and_retry
@limits(calls=MAX_REQUESTS_PER_MINUTE, period=ONE_MINUTE)
def rate_limited_api_call(func):
    return func()


class YouTubeAPI:
    """
    A client for interacting with the YouTube Data API v3.
    
    Provides methods for fetching channel information, videos, and comments.
    Handles API rate limiting and error handling.
    
    Args:
        api_key (str): Valid YouTube Data API key
        
    Raises:
        ValueError: If api_key is empty or invalid
    """
    
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Invalid API key")
        self.youtube = build("youtube", "v3", developerKey=api_key)

    def get_channel_id(self, handle: str) -> str:
        """
        Retrieves channel ID from a YouTube handle.
        
        Args:
            handle (str): YouTube channel handle (without @ symbol)
            
        Returns:
            str: YouTube channel ID
            
        Raises:
            ValueError: If no channel found for the given handle
        """
        response = rate_limited_api_call(
            lambda: self.youtube.search().list(q=handle, type="channel", part="id").execute()
        )
        
        if not response.get("items"):
            raise ValueError(f"No channel found: {handle}")
        return response["items"][0]["id"]["channelId"]

    def get_videos(self, channel_id: str) -> List[str]:
        """
        Retrieves all video IDs for a given channel.
        
        Handles pagination to fetch all available videos.
        
        Args:
            channel_id (str): YouTube channel ID
            
        Returns:
            List[str]: List of video IDs
            
        Raises:
            HttpError: If API request fails
        """
        def fetch_videos(token=None):
            return self.youtube.search().list(
                channelId=channel_id,
                part="id",
                maxResults=50,
                pageToken=token,
                type="video"
            ).execute()
            
        videos = []
        next_page_token = None
        
        while True:
            try:
                response = rate_limited_api_call(lambda t=next_page_token: fetch_videos(t))
                videos.extend([item["id"]["videoId"] for item in response["items"]])
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
            except HttpError as e:
                logger.error(f"Error fetching videos: {e}")
                raise
                
        return videos

    def get_video_details(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetches detailed information for a list of videos.
        
        Handles batching of requests (50 videos per request).
        
        Args:
            video_ids (List[str]): List of YouTube video IDs
            
        Returns:
            List[Dict[str, Any]]: List of video details including:
                - video_id (str)
                - title (str)
                - description (str)
                - published_date (datetime)
                - view_count (int)
                - like_count (int)
                - comment_count (int)
                - duration (str)
                - thumbnail_url (str)
        """
        video_data = []

        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i : i + 50]
            response = (
                self.youtube.videos()
                .list(part="snippet,statistics,contentDetails", id=",".join(chunk))
                .execute()
            )

            for item in response["items"]:
                video_data.append(self._parse_video_response(item))

        return video_data

    def get_comments(self, video_id: str, max_comments: int) -> List[Dict[str, Any]]:
        """
        Retrieves comments for a specific video.
        
        Args:
            video_id (str): YouTube video ID
            max_comments (int): Maximum number of comments to fetch
            
        Returns:
            List[Dict[str, Any]]: List of comments including:
                - video_id (str)
                - comment_id (str)
                - text (str)
                - author (str)
                - published_date (datetime)
                - like_count (int)
                - reply_to (str, optional): Parent comment ID for replies
        """
        comments = []
        try:
            response = (
                self.youtube.commentThreads()
                .list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=min(100, max_comments),
                )
                .execute()
            )

            for item in response["items"]:
                comments.extend(self._parse_comment_response(item, video_id))
                if len(comments) >= max_comments:
                    break

        except HttpError as e:
            logger.warning(f"Failed to fetch comments for {video_id}: {e}")

        return comments[:max_comments]

    def _parse_video_response(self, item: Dict) -> Dict[str, Any]:
        """
        Parses raw video API response into standardized format.
        
        Args:
            item (Dict): Raw API response for a video
            
        Returns:
            Dict[str, Any]: Parsed video data
        """
        return {
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "description": item["snippet"].get("description", ""),
            "published_date": datetime.fromisoformat(
                item["snippet"]["publishedAt"].replace("Z", UTC_Z)
            ),
            "view_count": int(item["statistics"].get("viewCount", 0)),
            "like_count": int(item["statistics"].get("likeCount", 0)),
            "comment_count": int(item["statistics"].get("commentCount", 0)),
            "duration": item["contentDetails"]["duration"],
            "thumbnail_url": item["snippet"]["thumbnails"]["high"]["url"],
        }

    def _parse_comment_response(
        self, item: Dict, video_id: str
    ) -> List[Dict[str, Any]]:
        """
        Parses raw comment API response into standardized format.
        
        Handles both top-level comments and replies.
        
        Args:
            item (Dict): Raw API response for a comment thread
            video_id (str): Associated YouTube video ID
            
        Returns:
            List[Dict[str, Any]]: List of parsed comments
        """
        comments = []
        comment = item["snippet"]["topLevelComment"]["snippet"]
        comments.append(
            {
                "video_id": video_id,
                "comment_id": item["id"],
                "text": comment["textDisplay"],
                "author": comment["authorDisplayName"],
                "published_date": datetime.fromisoformat(
                    comment["publishedAt"].replace("Z", UTC_Z)
                ),
                "like_count": int(comment["likeCount"]),
                "reply_to": None,
            }
        )

        if "replies" in item:
            for reply in item["replies"]["comments"]:
                reply_snippet = reply["snippet"]
                comments.append(
                    {
                        "video_id": video_id,
                        "comment_id": reply["id"],
                        "text": reply_snippet["textDisplay"],
                        "author": reply_snippet["authorDisplayName"],
                        "published_date": datetime.fromisoformat(
                            reply_snippet["publishedAt"].replace("Z", UTC_Z)
                        ),
                        "like_count": int(reply_snippet["likeCount"]),
                        "reply_to": item["id"],
                    }
                )
        return comments


class YouTubeDataFetcher:
    """
    High-level interface for fetching YouTube channel data.
    
    Combines video and comment data into pandas DataFrames.
    
    Args:
        api_key (str): Valid YouTube Data API key
    """
    
    def __init__(self, api_key: str):
        self.api = YouTubeAPI(api_key)

    def fetch_data(
        self, channel_url: str, max_comments: int = 100
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetches all videos and comments for a YouTube channel.
        
        Args:
            channel_url (str): Full YouTube channel URL
            max_comments (int, optional): Maximum comments to fetch per video. Defaults to 100.
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (videos_df, comments_df)
                videos_df contains video metadata
                comments_df contains comment data with video relationships
        """
        channel_input = YouTubeChannelInput(
            url=channel_url, handle=channel_url.split("@")[-1]
        )
        channel_id = self.api.get_channel_id(channel_input.handle)
        video_ids = self.api.get_videos(channel_id)

        videos_raw = self.api.get_video_details(video_ids)
        videos = pd.DataFrame([Video(**v).model_dump() for v in videos_raw])

        comments_raw = []
        for video_id in video_ids:
            comments_raw.extend(self.api.get_comments(video_id, max_comments))

        comments_data = []
        for c in comments_raw:
            comment = Comment(**c).model_dump()
            comment["published_date"] = comment["published_date"].replace(tzinfo=None)
            comments_data.append(comment)
        comments = pd.DataFrame(comments_data)

        videos["published_date"] = videos["published_date"].apply(
            lambda x: x.replace(tzinfo=None)
        )

        return videos, comments

def setup_cli() -> tuple[str, str, int, bool]:
    parser = argparse.ArgumentParser(description="Fetch YouTube channel data")
    parser.add_argument("channel_url", help="YouTube channel URL")
    parser.add_argument("-o", "--output", default="youtube_data.xlsx")
    parser.add_argument("-c", "--max-comments", type=int, default=100)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--api-key", help="YouTube API key")

    args = parser.parse_args()
    api_key = args.api_key or os.getenv(
        "YOUTUBE_API_KEY"
    )

    if not api_key:
        logger.error("No API key provided")
        sys.exit(1)

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    return api_key, args.channel_url, args.max_comments, args.output


def main():
    api_key, channel_url, max_comments, output_file = setup_cli()

    try:
        fetcher = YouTubeDataFetcher(api_key)
        videos, comments = fetcher.fetch_data(channel_url, max_comments)

        with pd.ExcelWriter(output_file) as writer:
            videos.to_excel(writer, sheet_name="Video Data", index=False)
            comments.to_excel(writer, sheet_name="Comments Data", index=False)

        logger.info(f"Data exported to {output_file}")

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
