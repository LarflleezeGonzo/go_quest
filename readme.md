# YouTube Data Fetcher

A Python CLI tool to fetch video and comment data from YouTube channels.

## Features
- Fetches basic data for all videos on a channel
- Collects latest comments and replies across videos
- Rate-limited API calls to respect YouTube quotas
- Data validation using Pydantic
- Exports to Excel with separate sheets for videos and comments

## Installation
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

## Configuration
Set YouTube API key:
```bash
export YOUTUBE_API_KEY='api_key_example'
```

## Usage
```bash
python youtube.py https://youtube.com/@ChannelHandle

python youtube.py https://youtube.com/@ChannelHandle -o output.xlsx -c 200 -v
```

Options:
- `-o, --output`: Output Excel file path (default: youtube_data.xlsx)
- `-c, --max-comments`: Maximum comments to fetch (default: 100)
- `-v, --verbose`: Enable debug logging
- `--api-key`: Override environment variable API key

## Output Format
### Video Data Sheet
- Video ID
- Title
- Description
- Published date
- View count
- Like count
- Comment count
- Duration
- Thumbnail URL

### Comments Data Sheet
- Video ID
- Comment ID
- Text
- Author
- Published date
- Like count
- Reply to (for comment replies)

## Requirements
See requirements.txt for package dependencies:
```
google-api-python-client
pandas
pydantic
openpyxl
ratelimit
```