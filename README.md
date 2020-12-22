# Description
This project was created for the purpose of analyzing data for the music streaming app ***Sparkify***. In particular, the analysis team is interested in understanding what songs the users are listening to. Their user base has grown and they want to transfer their data warehouse to the cloud. This is done by building an ETL pipeline in python to transfer data from JSON files on **S3** to **Redshift** where it is transformed into a stars schema. The analysis team can use this data warehouse to find what songs their users are listening to. 

# How to Run the Project
1. Create an AWS **IAM user** with **AdministratorAccess**
2. Copy the access key and secret into the dwg.cfg file 

       [AWS]
       KEY= 
       SECRET= 
       
        
3. Launch a **Redshift cluster** and create and **IAM role** that has read access to S3. This can be done using the **IaC.ipynb** notebook. Follow the steps before Step 5. 
   After this step is done the following should be filled in **dwh.cfg**

       [CLUSTER]
       host =
        
       [IAM_ROLE]
       arn =
       

4. In the terminal run the following commands to create and populate the staging and dimensional tables.
     
       python create_tables.py
       python etl.py


5. To clean up resources follow **Step 5** in **IaC.ipynb** notebook which will delete the cluster and IAM role.


# File Description 
## Repository
* **dwh.cfg** - contains the parameters and credentials needed to create and connect to the cluster and database. It also contains the S3 data paths. 

* **IaC.ipynb** -*adapted from notebook in excercise 2*- contains steps to create and delete a Redshift Cluster and IAM role using the AWS python SDK

* **sql_queries.py** - contains all sql statments needed to drop, create and insert into staging and dimensional tables.

* **create_tables.py** - creates a connection to the cluster, drops and creates staging and dimensional tables.

* **etl.py** - Loads data into the staging tables, and transforms and loads data into the dimensional tables.

* **README.md** - contains a description of the project.

## S3

* **Song data**: *s3://udacity-dend/song_data* - contains metadata about a song and the artist of that song in JSON format.

* **Log data**: *s3://udacity-dend/log_data* - contains simulated app activity logs from an imaginary music streaming app based on configuration settings.
   
* **Log data json path**: *s3://udacity-dend/log_json_path.json* - used to map the source data to the table columns



# Database Schema and ETL Pipline
## Staging Tables
Two staging tables are created to move the data from S3 to Redshift. 
1. **staging_events** - created to hold data from the log JSON files
    * artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration,  sessionId, song, status, ts, userAgent, userId
    * Data is moved to this table from S3 using the *copy* command with the log *JSON path*
    
    
2. **staging_songs** - created to hold data from the song JSON files
   * num_songs, artist_id, artist_latitude, artist_longitud, artist_location, artist_name, song_id, title, duration, year 
   * Data is moved to this table from S3 using the *copy* command

## Star Schema
Data is transformed within Redshift to a star schema from the staging tables
### Fact Table
**songplays** - records in event data associated with song plays i.e. records with page NextSong - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Data is transfered into this table by *joining* the two staging table using songId.

### Dimension Tables
* **users** - users in the app (user_id, first_name, last_name, gender, level)
  
  Data is transfered into this table using *insert into .. select* from the *staging_events* table
  
  
* **songs** - songs in music database (song_id, title, artist_id, year, duration)
  
  Data is transfered into this table using *insert into .. select* from the *staging_songs* table
  
  
* **artists** - artists in music database (artist_id, name, location, lattitude, longitude)
  
  Data is transfered into this table using *insert into .. select* from the *staging_songs* table
  
  
* **time** - timestamps of records in songplays broken down into specific units
  (start_time, hour, day, week, month, year, weekday)
  
  start_time is populated using *insert into .. select* from the *staging_events* table and the rest of the columns are populated using the *extract()* function to get the time units
  
  

