from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import csv
import os
from dotenv import load_dotenv
from datetime import datetime
import sqlite3
import pandas as pd
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

# Interested Playlists
SKIENNA_PLAYLISTS = (
    'PLOtl7M3yp-DX32N0fVIyvn7ipWKNGmwpp',  # Analysis of Algorithms 2016
    'PLOtl7M3yp-DVBdLYatrltDJr56AKZ1qXo',  # Data Science - Fall 2016
)

MIT_PLAYLISTS = (
    'PLE18841CABEA24090',  # Structure and Interpretation
    'PLE7DDD91010BC51F8',  # Linear Algebra
    'PLUl4u3cNGP61Oq3tWYp6V_F-5jb5L2iHb',  # Introduction to Algorithms
    # Introduction to Computer Science and Programming - Fall 2016
    'PLUl4u3cNGP63WbdFxL8giv4yhgdMGaZNA',
    # Learn to Build your own video game with the Unity Game Engine and MS Kinect
    'PLUl4u3cNGP60ZaGv5SgpIk67YnH1WqCLI',
    'PLF83B8D8C87426E44',  # Fundamentals of Biology
    # Introduction to Computational Thinking and Data Science
    'PLUl4u3cNGP619EG1wp0kT-7rDE_Az5TNd',
    'PLUl4u3cNGP61-9PEhRognw5vryrSEVLPr',  # Quantum Physics I, 2013
    # Introduction to Computer Science and Programming - Fall 2008 (most popular)
    'PL4C4720A6F225E074',
    # MIT 18.S096 Topics in Mathematics w Applications in Finance
    'PLUl4u3cNGP63ctJIEC1UnZ0btsphnnoHR',
)

THREE_BLUE_ONE_BROWN_PLAYLIST = (
    'PLZHQObOWTQDPD3MizzM2xVFitgF8hE_ab',  # Essence of Linear Algebra
    'PLZHQObOWTQDMsr9K-rj53DwVRMYO3t5Yr',  # Essence of Calculus
    'PLZHQObOWTQDNU6R1_67000Dx_ZCJB-3pi',  # Neural networks
    'PLZHQObOWTQDNPOjrT6KVlfJuKtYTftqH6',  # Differential Equations
)

HARVARD_UNIVERSITY_PLAYLIST = (
    'PL2SOU6wwxB0v1kQTpqpuu5kEJo2i-iUyf',  # Algorithms for Big Data
    # Advanced Algorithms (COMPSCI 224) (popular)
    'PL2SOU6wwxB0uP4rJgf5ayhHWgw7akUWSf',
)

COURSEA_PLAYLIST = (
    # GCP Fundamentals - Google Cloud Platform Fundamentals: Core Infrastructure
    'PLVext98k2evjIFqVggHfvecnFu4tTJK_o',
    # R Programming - Introduction to R by Johns Hopkins University
    'PLVext98k2evi8mDNRo4MwIgVgSmwM3cS8',
)

KHAN_ACADEMY_PLAYLIST = (
    'PLSQl0a2vh4HA50QhFIirlEZRXG4yjcoGM',  # Journey into cryptography
)

FREECODECAMP_PLAYLIST = (
    # Introduction to Computer Science, Harvard's CS50
    'PLWKjhJtqVAbmGw5fN5BQlwuug-8bDmabi',
    'PLWKjhJtqVAbluXJKKbCIb4xd7fcRkpzoz',  # Introduction to Game Development
)

OTHERS_PLAYLIST = (
    'PL6cactdCCnTLkQah9GKzsJmiLbegy4dEk',  # Udemy Ultimate Web Development Tutorial
    'PLC3y8-rFHvwgg3vaYJgHGnModB54rxOk3',  # Codevolution - Introduction to React
)

list_of_playlists = SKIENNA_PLAYLISTS + MIT_PLAYLISTS + \
                    THREE_BLUE_ONE_BROWN_PLAYLIST + HARVARD_UNIVERSITY_PLAYLIST + \
                    COURSEA_PLAYLIST + KHAN_ACADEMY_PLAYLIST + \
                    FREECODECAMP_PLAYLIST + OTHERS_PLAYLIST

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
                    f'Video ${playlist_videos[i]} has no significant movement. Moving on to next video…')
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

    for i in set(list_of_playlists).difference(set(current_playlists)):
        if i is not None:
            get_playlist_metadata(g_service, connection, i)

    for i in range(len(list_of_playlists)):
        get_playlist_videos_data(g_service, connection, list_of_playlists[i])