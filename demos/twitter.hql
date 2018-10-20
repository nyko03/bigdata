--set hive.exec.dynamic.partition = true;
--set hive.exec.dynamic.partition.mode = nonstrict;
CREATE DATABASE tweetsdb;
USE tweetsdb;

DROP TABLE IF EXISTS tweets_raw;
CREATE EXTERNAL TABLE tweets_raw (
    json_response STRING
)
STORED AS TEXTFILE LOCATION '/HdiSamples/HdiSamples/TwitterTrendsSampleData';
SELECT * from tweets_raw;
DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets
(
    id BIGINT,
    created_at STRING,
    created_at_date STRING,
    created_at_year STRING,
    created_at_month STRING,
    created_at_day STRING,
    created_at_time STRING,
    in_reply_to_user_id_str STRING,
    text STRING,
    contributors STRING,
    retweeted STRING,
    truncated STRING,
    coordinates STRING,
    source STRING,
    retweet_count INT,
    url STRING,
    hashtags array<STRING>,
    user_mentions array<STRING>,
    first_hashtag STRING,
    first_user_mention STRING,
    screen_name STRING,
    name STRING,
    followers_count INT,
    listed_count INT,
    friends_count INT,
    lang STRING,
    user_location STRING,
    time_zone STRING,
    profile_image_url STRING,
    json_response STRING
);

FROM tweets_raw
INSERT OVERWRITE TABLE tweets
SELECT
    cast(get_json_object(json_response, '$.id_str') as BIGINT),
    get_json_object(json_response, '$.created_at'),
    concat(substr (get_json_object(json_response, '$.created_at'),1,10),' ',
    substr (get_json_object(json_response, '$.created_at'),27,4)),
    substr (get_json_object(json_response, '$.created_at'),27,4),
    case substr (get_json_object(json_response, '$.created_at'),5,3)
        when "Jan" then "01"
        when "Feb" then "02"
        when "Mar" then "03"
        when "Apr" then "04"
        when "May" then "05"
        when "Jun" then "06"
        when "Jul" then "07"
        when "Aug" then "08"
        when "Sep" then "09"
        when "Oct" then "10"
        when "Nov" then "11"
        when "Dec" then "12" end,
    substr (get_json_object(json_response, '$.created_at'),9,2),
    substr (get_json_object(json_response, '$.created_at'),12,8),
    get_json_object(json_response, '$.in_reply_to_user_id_str'),
    get_json_object(json_response, '$.text'),
    get_json_object(json_response, '$.contributors'),
    get_json_object(json_response, '$.retweeted'),
    get_json_object(json_response, '$.truncated'),
    get_json_object(json_response, '$.coordinates'),
    get_json_object(json_response, '$.source'),
    cast (get_json_object(json_response, '$.retweet_count') as INT),
    get_json_object(json_response, '$.entities.display_url'),
    array(
        trim(lower(get_json_object(json_response, '$.entities.hashtags[0].text'))),
        trim(lower(get_json_object(json_response, '$.entities.hashtags[1].text'))),
        trim(lower(get_json_object(json_response, '$.entities.hashtags[2].text'))),
        trim(lower(get_json_object(json_response, '$.entities.hashtags[3].text'))),
        trim(lower(get_json_object(json_response, '$.entities.hashtags[4].text')))),
    array(
        trim(lower(get_json_object(json_response, '$.entities.user_mentions[0].screen_name'))),
        trim(lower(get_json_object(json_response, '$.entities.user_mentions[1].screen_name'))),
        trim(lower(get_json_object(json_response, '$.entities.user_mentions[2].screen_name'))),
        trim(lower(get_json_object(json_response, '$.entities.user_mentions[3].screen_name'))),
        trim(lower(get_json_object(json_response, '$.entities.user_mentions[4].screen_name')))),
    trim(lower(get_json_object(json_response, '$.entities.hashtags[0].text'))),
    trim(lower(get_json_object(json_response, '$.entities.user_mentions[0].screen_name'))),
    get_json_object(json_response, '$.user.screen_name'),
    get_json_object(json_response, '$.user.name'),
    cast (get_json_object(json_response, '$.user.followers_count') as INT),
    cast (get_json_object(json_response, '$.user.listed_count') as INT),
    cast (get_json_object(json_response, '$.user.friends_count') as INT),
    get_json_object(json_response, '$.user.lang'),
    get_json_object(json_response, '$.user.location'),
    get_json_object(json_response, '$.user.time_zone'),
    get_json_object(json_response, '$.user.profile_image_url'),
    json_response
WHERE (length(json_response) > 500);

-- Guardamos el resultado de una consulta
INSERT OVERWRITE DIRECTORY '/datos'
SELECT name, screen_name, count(1) as cc
    FROM tweets
    WHERE text like "%Azure%"
    GROUP BY name,screen_name
    ORDER BY cc DESC LIMIT 10;

--SELECT * FROM tweets;