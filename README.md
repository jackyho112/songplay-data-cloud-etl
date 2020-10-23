# Data Modeling with Redshift

In this project, we are extracting data regarding song plays from JSON files into an Amazon Redshift data warehouse.

## Dataset 

The dataset comes from the [Million Song Dataset](http://millionsongdataset.com/). The data should contain two folders, one for the log and one for the songs. The song dataset should contain metadata about a song and its artist. The log dataset should contain acitivity records of played songs.

## Schema

For the database, we are creating the following tables.

songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
users - user_id, first_name, last_name, gender, level
songs - song_id, title, artist_id, year, duration
artists - artist_id, name, location, latitude, longitude
time - start_time, hour, day, week, month, year, weekday

## Creating a Redshift cluster

`create_redshift_cluster.ipynb` contains instructions on how to create a Redshift server programmatically.

## Running the scripts

This project contains two scripts, `create_tables.py` and `etl.py`. The former creates the necessary tables, while the latter runs the pipeline to save the records extracted from the JSON files. Both scripts require dwh.cfg to be filled before being run.

To run the scripts, run the following code in your terminal:
```bash
python create_tables.py
python etl.py
```

or the following in your notebook:
```python
!python create_tables.py
!python etl.py
```