CREATE OR REPLACE TABLE raw_albums (
    album_id          VARCHAR                PRIMARY KEY,
    album_name        VARCHAR(200)           NOT NULL,
    album_rel_dt      DATE                   DEFAULT TO_DATE('1900-01-01') NOT NULL,
    album_tot_tracks  NUMBER(20)             NOT NULL,
    album_url         VARCHAR(500)           NOT NULL,
    album_load_time   TIMESTAMP              DEFAULT CURRENT_TIMESTAMP 
);

CREATE OR REPLACE TABLE raw_artists (
    artist_id          VARCHAR               PRIMARY KEY,
    artist_name        VARCHAR(200)          NOT NULL,
    artist_url         VARCHAR(500)          NOT NULL,
    artist_load_time   TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE raw_songs (
    song_id           VARCHAR                PRIMARY KEY,
    song_name         VARCHAR(200)           NOT NULL,
    song_duration     NUMBER                 NOT NULL,
    song_url          VARCHAR(500)           NOT NULL,
    song_popularity   NUMBER                 NOT NULL,
    song_added        DATE                   DEFAULT TO_DATE('1900-01-01'),
    album_id          VARCHAR                NOT NULL,
    artist_id         VARCHAR                NOT NULL,
    CONSTRAINT song_fk_album FOREIGN KEY (album_id) REFERENCES raw_albums(album_id),
    CONSTRAINT song_fk_artist FOREIGN KEY (artist_id) REFERENCES raw_artists(artist_id),
    song_load_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


Create or replace storage integration AWS_S3_INTEGRATION
Type = external_stage  
storage_provider = S3  
enabled = true
storage_aws_role_arn = 'arn:aws:iam::1111111111:role/S3_AUTH_ROLE_AWW'
storage_allowed_locations = ('s3://spot-bucket85/transformed_data/')
comment = 'Name        : AWS_S3_INTEGRATION
           Created By  : Debaraj Lenka
           Created Date: 01-10-2025';


DESC integration AWS_S3_INTEGRATION;

create or replace file format csv_format_latest
type = csv
field_delimiter = ','
skip_header = 1 
null_if = ('NULL', 'null')  
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
empty_field_as_null = true;

create or replace stage spotify_data_stage_album  
storage_integration = AWS_S3_INTEGRATION  
url = 's3://spotify-api-bucket85/transformed_data/album_data/'
file_format = csv_format_latest;


create or replace stage spotify_data_stage_artist  
storage_integration = AWS_S3_INTEGRATION  
url = 's3://spotify-api-bucket85/transformed_data/artist_data/'
file_format = csv_format_latest;


create or replace stage spotify_data_stage_song 
storage_integration = AWS_S3_INTEGRATION  
url = 's3://spotify-api-bucket85/transformed_data/songs_data/'
file_format = csv_format_latest;




CREATE OR REPLACE NOTIFICATION INTEGRATION my_aws_sns_int
TYPE = QUEUE
ENABLED = TRUE
DIRECTION = OUTBOUND
NOTIFICATION_PROVIDER = 'AWS_SNS'
AWS_SNS_ROLE_ARN = 'arn:aws:iam::11111111111:role/S3_AUTH_ROLE_AWW'
AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:11111111111:spotify_notification';





list @spotify_data_stage


CREATE OR REPLACE PIPE album_pipe  
AUTO_INGEST = TRUE
ERROR_INTEGRATION = my_aws_sns_int  AS
COPY INTO spotifydb.dev.raw_albums  FROM (
SELECT
t.$1 AS ALBUM_ID,  
t.$2 AS ALBUM_NAME,
t.$3 as ALBUM_REL_DT,  
t.$4 AS ALBUM_TOT_TRACKS,  
t.$5 AS ALBUM_URL,
CURRENT_TIMESTAMP AS load_time  FROM @spotify_data_stage_album t
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format_latest');



CREATE OR REPLACE PIPE artist_pipe  
AUTO_INGEST = TRUE
ERROR_INTEGRATION = my_aws_sns_int  AS
COPY INTO spotifydb.dev.raw_artists  FROM (
SELECT
t.$1 AS ARTIST_ID,  
t.$2 AS ARTIST_NAME,
t.$3 as ARTIST_URL,  
CURRENT_TIMESTAMP AS load_time,  FROM @spotify_data_stage_artist t
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format_latest');



CREATE OR REPLACE PIPE song_pipe  
AUTO_INGEST = TRUE
ERROR_INTEGRATION = my_aws_sns_int  AS
COPY INTO spotifydb.dev.raw_songs  FROM (
SELECT
t.$1 AS SONG_ID,  
t.$2 AS SONG_NAME,
t.$3 as SONG_DURATION,  
t.$4 AS SONG_URL,  
t.$5 AS SONG_POPULARITY,
t.$6 as SONG_ADDED,  
t.$7 AS ALBUM_ID,  
t.$8 AS ARTIST_ID,
CURRENT_TIMESTAMP AS load_time,  FROM @spotify_data_stage_song t
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format_latest');



ALTER PIPE SPOTIFYDB.DEV.album_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
ALTER PIPE SPOTIFYDB.DEV.artist_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
ALTER PIPE SPOTIFYDB.DEV.song_pipe SET PIPE_EXECUTION_PAUSED = FALSE;


SELECT SYSTEM$PIPE_STATUS('album_pipe');
SELECT SYSTEM$PIPE_STATUS('artist_pipe');
SELECT SYSTEM$PIPE_STATUS('song_pipe');

SELECT SYSTEM$PIPE_FORCE_RESUME('album_pipe');
SELECT SYSTEM$PIPE_FORCE_RESUME('artist_pipe');
SELECT SYSTEM$PIPE_FORCE_RESUME('song_pipe');

ALTER PIPE album_pipe REFRESH;
ALTER PIPE artist_pipe REFRESH;
ALTER PIPE song_pipe REFRESH;



SELECT *
FROM TABLE(
  VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'spotifydb.dev.album_pipe',
    START_TIME => DATEADD(hour, -60, CURRENT_TIMESTAMP())
  )
);




SELECT 
 *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  DATE_RANGE_START => DATEADD(hour, -30, CURRENT_TIMESTAMP),
  PIPE_NAME => 'album_pipe'
))
ORDER BY START_TIME DESC;



show pipes;

