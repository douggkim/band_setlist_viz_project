import pandas as pd
import polars as pl
from utils.data_processing import camel_to_snake


def process_raw(result_df:pd.DataFrame) -> tuple[pd.DataFrame]: 
    """
    divides the raw setlist data into different dataframes for venues, artists and sets
    args: 
        result_df: the raw data to be processed
    returns: 
        tuple(artists_df, venues_df, sets_df, results_df_2)
    """
    result_df_2 = result_df.copy() 
    # Rename id 
    result_df_2 = result_df_2.rename(columns={"id":"live_id"})

    # Split the result dataframe to artists 
    artists_df = result_df_2[["artist"]] 
    result_df_2["artist_id"] = result_df_2["artist"].apply(lambda x:x["mbid"] if isinstance(x,dict) else None)
    result_df_2 = result_df_2.drop(["artist"], axis=1) 
    # Split the result dataframe to venues 
    venues_df = result_df_2[["venue"]]
    result_df_2["venue_id"] = result_df_2["venue"].apply(lambda x:x["id"] if isinstance(x,dict) else None)
    result_df_2 = result_df_2.drop(["venue"], axis=1) 
    # split the result dataframe to sets
    sets_df = result_df_2[["live_id","sets"]]
    result_df_2 = result_df_2.drop(["sets"], axis=1)

    # split the result dataframe to sets
    result_df_2["tour_name"] = result_df_2["tour"].apply(lambda x: x["name"] if isinstance(x,dict) else None)
    result_df_2 = result_df_2.drop(["tour"], axis=1)
    
    return artists_df, venues_df, sets_df, result_df_2


def process_artist(df:pd.DataFrame) -> pd.DataFrame: 
    """
    process the data frame for artists (remove duplicates
    args: 
        df: df to be processed 
    returns: 
        df: processed version of artists_df 
    """ 
    df = pd.json_normalize(df["artist"])
    df = df.drop_duplicates()
    df.columns = [camel_to_snake(col) for col in df.columns]
    
    return df

def process_venue(df:pd.DataFrame) -> pd.DataFrame: 
    """
    process the data frame for venues (remove duplicates, process column names)
    args: 
        df: df to be processed 
    returns: 
        df: processed version of venues df
    """ 
    df = pd.json_normalize(df["venue"])
    df = df.drop_duplicates(subset=["id"])
    df.columns = [camel_to_snake(col) for col in df.columns]
    
    return df


def process_set(sets_df:pd.DataFrame) -> pd.DataFrame: 
    """
    processes, explodes, and normalizes sets data
    args: 
        sets_df(pd.DataFrame): the raw sets data to be processed 
    returns: 
        sets_df(pd.DataFrame): processed data
    """ 
    sets_df["sets"] = sets_df["sets"].apply(lambda x:x["set"] if isinstance(x,dict) else None)

    sets_df = sets_df.explode(column=["sets"])
    tmp_sets = pd.json_normalize(sets_df["sets"]).reset_index(drop=True)
    sets_df = pd.concat([sets_df.reset_index(drop=True),tmp_sets], axis=1)

    sets_df = sets_df.drop(["sets"], axis=1)
    sets_df = sets_df.rename(columns={"name":"album_name"})

    sets_df = sets_df.explode(column=["song"]).reset_index(drop=True)
    songs_df = pd.json_normalize(sets_df["song"]).reset_index(drop=True) 

    sets_df = pd.concat([sets_df, songs_df], axis=1)

    sets_df.columns = [camel_to_snake(col) for col in sets_df.columns]
    sets_df = sets_df.rename(columns={"name":"song_name"})
    sets_df = sets_df.drop(["song"],axis=1)
    
    return sets_df