# Project Overview
Retrieves a band's setlist data using setlist.fm's REST API.

[setlist.fm REST API Documentation](https://api.setlist.fm/docs/1.0/index.html)

# Description per stage 
## Preparation
1. Get the API Key from setlist.fm. Link in the REST API documentation
2. Save the key to `_credentials.json` file as below 
```
{
    "x-api-key": {api key you got from setlist.fm}
}
```
## Data Ingestion
1. Retreives data from two endpoints: 
    - `/1.0/artist/{mbid}`: get the id (`mbid`) of the desired artist
    - `/1.0/artist/{mbid}/setLists`: get the venue, setlist, concerts done by the artist (`mbid`)
2. Uses asyncio to make asynchronous REST API calls. 
    - the parameter for rate limit can be changed in /transformations/data_retrieval.py/fetch_and_process data function

## Data Transformation
1. Splits the data from `/1.0/artists/{mbid}/setLists` REST API endpoint into four tables
    - `meta`: overall information about a concert, could be joined with artists, venues, sets
    - `artists`: information about the artists. To be joined with data from Spotify API for data enrichment
    - `venues`: information about the concert locations 
    - `sets`: information about songs played during each concert. Contains information about whether it's a cover. 

## Data Storage
_WIP_ 
## Data Reporting
_WIP_
