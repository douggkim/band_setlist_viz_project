from transformations.data_retrieval import search_artist, get_setlist
from transformations.data_transformation import process_artist, process_raw, process_set, process_venue
import json 
import asyncio 


## Retreive credentials from file
json_file_name = "./_credentials.json"

with open(json_file_name,"r") as json_file: 
    raw_json = json.load(json_file)
    api_key = raw_json["x-api-key"]
    
base_url = "https://api.setlist.fm/rest/1.0" 
headers = {
    "x-api-key": api_key,
    "Accept" : "application/json"
}
## Get the artist to search for
artist_name = "ahmad jamal" # Should be changed later to user input 
mbid = search_artist(artist_name=artist_name, headers=headers)

async def main(): 
    ## Get raw data
    result_df = await get_setlist(mbid=mbid, headers=headers)
    
    ## Divide the raw data into multiple dataframes 
    artists_df, venues_df, sets_df, result_df_2 = process_raw(result_df)
    
    ## Process each dataframe accordingly 
    artists_df = process_artist(artists_df)
    venues_df = process_venue(venues_df) 
    sets_df = process_set(sets_df)
    
    
if __name__ == '__main__':
    asyncio.run(main())