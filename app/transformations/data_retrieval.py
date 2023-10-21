import requests
import pandas as pd
import polars as pl
import json
import math
import aiohttp
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo

def search_artist(artist_name:str, headers:dict, base_url:str="https://api.setlist.fm/rest/1.0"): 
    """
    returns the mbid for the artist on setlist.fm
    args
        artist_name(str): name of the artist to be searched 
    returns
        mbid(str): mbid of the artist
    """
    raw_response_artist = requests.get(url=base_url+f"/search/artists?artistName={artist_name}", headers=headers).content
    artist_json = json.loads(raw_response_artist)
    
    for artist in artist_json["artist"]:
        if artist["name"].lower() == artist_name:
            mbid = artist["mbid"]
    
    return mbid


@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=1, period=1) # 1 request per second
async def fetch_and_process_data(session, url, headers):
    async with session.get(url, headers=headers) as response:
        data = await response.text()
        data_json = json.loads(data) 
        return data_json

async def get_setlist(mbid:str, headers:dict, base_url:str="https://api.setlist.fm/rest/1.0"): 
    async with aiohttp.ClientSession() as session:
        raw_response_setlist = await fetch_and_process_data(session, base_url + f"/artist/{mbid}/setlists")

    # Get call_num
    call_num = math.ceil(raw_response_setlist["total"] / raw_response_setlist["itemsPerPage"])

    # Load to dataframe
    result_df = pd.DataFrame(raw_response_setlist["setlist"]).reset_index(drop=True)

    call_cnt = 2

    # Call the number of pages
    while call_cnt <= call_num:
        async with aiohttp.ClientSession() as session:
            url = base_url + f"/artist/{mbid}/setlists?p={call_cnt}"
            print(f"Fetching data for {mbid}: getting {call_cnt} out of {call_num} pages")
            raw_response = await fetch_and_process_data(session, url, headers)

        tmp_df = pd.DataFrame(raw_response["setlist"]).reset_index(drop=True)
        result_df = pd.concat([result_df, tmp_df], axis=0)
        call_cnt += 1
    return result_df
