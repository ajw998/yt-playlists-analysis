from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import csv
import os
from dotenv import load_dotenv
from datetime import datetime
import sqlite3
import pandas as pd
from playlists import PLAYLISTS
load_dotenv(verbose=True)

# Defining the fetch functions
# The free tier Youtube API has a maximum quota of 10,000 requests per day. When defining these fetch functions, our goal here is to reduce the quota costs.

# The combine cost for fetching a video's `snippet`, `contentDetails`, and `statistics` is 7 units. This means we can at most fetch 1,428 videos per day.

# The combine cost for fetching `playlistItem`'s and `playlist`'s `snippet` is 3 units. This means we can fetch 3,333 `playlistItem` or `playlist` per day.

# We can minimise this cost by only fetch the statistics, and check if there are any major shifts in the view count (we may set an arbitrary tolerance at around **5%**).

# We can reduce the quota cost by first checking the static parts of our data. For playlist details, this means checking whether the specific playlist is already inside the database.

# Constants
DEVELOPER_KEY = os.getenv('API_KEY')
SERVICE_NAME = "youtube"
VERSION = "v3"

# Requests
MAX_RESULTS = 50

DB = 'data/ytdata.db'

# Check if value exists in list
def exists_p(id, plist):
    return id in plist

# Percentage change
def pct_change(old, new):
    if (int(new) - int(old)) is not 0:
        return float(((int(new) - int(old)) * 100)/int(old))
    else:
        return 0

# Extract video ids from playlists data
def extract_video_id(data):
    return tuple(i['snippet']['resourceId']['videoId'] for i in data)

# Fetch
# Recursively fetch playlist item and returns a concatenated list of all video ids inside a playlist

def get_playlist_items(service, playlist_id, npt=''):
    res = service.playlistItems().list(
        part="snippet",
        fields="nextPageToken,items(snippet(resourceId(videoId)))",
        playlistId=playlist_id,
        maxResults=MAX_RESULTS,
        pageToken=npt
    ).execute()

    if res.get('nextPageToken') != None:
        return res['items'] + get_playlist_items(playlist_id, res['nextPageToken'])

    return res['items']

def get_video_data(service, video_id, parts='snippet', fields='items(snippet)'):
    res = service.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id,
        fields=fields,
        maxResults=MAX_RESULTS
    ).execute()
    return res

# Get playlist metadata
def get_playlist_details(service, playlist_id):
    res = service.playlists().list(
        part="snippet,contentDetails",
        fields="items(snippet(channelId, title, channelTitle, description), contentDetails)",
        id=playlist_id,
        maxResults=MAX_RESULTS
    ).execute()
    return res

def get_playlist_metadata(service, connection, playlist_id):
    playlist_details = get_playlist_details(service, playlist_id)['items'][0]
    connection.cursor().execute(
        f'INSERT INTO Playlist (id, title, description, channelTitle, itemCount) VALUES ("{playlist_id}", "{playlist_details["snippet"]["title"]}", "{playlist_details["snippet"]["description"]}", "{playlist_details["snippet"]["channelTitle"]}", "{playlist_details["contentDetails"]["itemCount"]}")')

def get_playlist_videos_data(service, connection, playlist_id):
    playlist = get_playlist_items(service, playlist_id)
    playlist_videos = extract_video_id(playlist)

    for i in range(len(playlist_videos)):
        print(f'Handling current video id {playlist_videos[i]}')
        if exists_p(playlist_videos[i], current_videos):
            old_count = pd.read_sql_query(
                f'SELECT views FROM Video WHERE id="%s"' % playlist_videos[i], connection)['views'][0]
            new_count = get_video_data(service, playlist_videos[i], 'statistics', 'items(statistics)')[
                            'items'][0]['statistics']['viewCount']
            if pct_change(old_count, new_count) >= 5:
                print(
                    f'Detected percentage change at {pct_change(old_count, new_count)}%, updating recording…')
                connection.cursor().execute(
                    f'UPDATE Video SET views={new_count} WHERE id="{playlist_videos[i]}"')
                connection.commit()
            else:
                print(
                    f'Video {playlist_videos[i]} has no significant movement. Moving on to next video…')
        else:
            print('New record found! Inserting new record…')
            data = get_video_data(service, playlist_videos[i], 'snippet,statistics,contentDetails',
                                  'items(id,snippet(title,categoryId,publishedAt),statistics,contentDetails(duration))')['items'][0]
            connection.cursor().execute(
                f'INSERT INTO Video (id, title, categoryId, playlistId, duration, views, uploadedDate) VALUES ("{data["id"]}", "{data["snippet"]["title"]}", {data["snippet"]["categoryId"]}, "{playlist_id}", "{data["contentDetails"]["duration"]}", {data["statistics"]["viewCount"]}, "{data["snippet"]["publishedAt"]}")')
            connection.commit()


if __name__ == '__main':
    connection = sqlite3.connect(DB, timeout=20)
    current_playlists = pd.read_sql_query(
        'SELECT id FROM Playlist', connection)['id'].tolist()
    current_videos = pd.read_sql_query(
        'SELECT id FROM Video', connection)['id'].tolist()
    g_service = build(SERVICE_NAME, VERSION, developerKey=DEVELOPER_KEY)

    for i in set(PLAYLISTS).difference(set(current_playlists)):
        if i is not None:
            get_playlist_metadata(g_service, connection, i)

    for i in range(len(PLAYLISTS)):
        get_playlist_videos_data(g_service, connection, PLAYLISTS[i])
