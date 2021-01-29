# Datawarehouse Project
### Project description
Sparkify is a music streaming startup with a growing user base and song database.

Their user activity and songs metadata data resides in json files in S3. The goal of the project is to build an ETL pipeline that will extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.
### Database Schema
Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables
- users - users in the app
    user_id, first_name, last_name, gender, level
- songs - songs in music database
    song_id, title, artist_id, year, duration
- artists - artists in music database
    artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday
### Structure
The project contains the following components:

- `create_tables.py` creates the Sparkify star schema in Redshift
- `etl.py` defines the ETL pipeline, extracting data from S3, loading into staging tables on Redshift, and then processing into analytics tables on Redshift
- `sql_queries.py` defines the SQL queries that underpin the creation of the star schema and ETL pipeline
### How to run?

1. To run this project you will need to fill the following information, and save it as dwh.cfg in the project root folder.
  > [CLUSTER]
  > HOST= 
  > DB_NAME=dwh
  > DB_USER=dwhuser
  > DB_PASSWORD=Passw0rd
  > DB_PORT=5439
  > [IAM_ROLE]
  > ARN= 
  > [S3]
  > LOG_DATA='s3://udacity-dend/log_data'
  > LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
  > SONG_DATA='s3://udacity-dend/song_data'
  > [AWS]
  > KEY=
  > SECRET=
  > [DWH] 
  > DWH_CLUSTER_TYPE       =multi-node
  > DWH_NUM_NODES          =4
  > DWH_NODE_TYPE          =dc2.large
  > DWH_IAM_ROLE_NAME      =dwhRole
  > DWH_CLUSTER_IDENTIFIER =dwhCluster
  > DWH_DB                 =dwh
  > DWH_DB_USER            =dwhuser
  > DWH_DB_PASSWORD        =Passw0rd
  > DWH_PORT               =5439  
2. Follow the steps in IaC notebook to set up the needed infrastructure for this project.
3. Run the create_tables script to set up the database staging and analytical tables.
    At the command prompt, type `python create_tables.py`.
4. Finally, run the etl script to extract data from the files in S3, stage it in redshift, and finally store it in the dimensional tables.
    At the command prompt, type `python etl.py`.
### Example queries
- Count how many male and female listn to the same song 
```
select a.name ,u.gender ,count(u.gender) 
from users u
JOIN songplays s ON s.user_id = u.user_id
JOIN artists a   ON s.artist_id = a.artist_id
group by a.name ,u.gender
order by a.name  ;
```