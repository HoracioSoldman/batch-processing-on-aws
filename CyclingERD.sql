DROP TABLE IF EXISTS "fact_journey";
DROP TABLE IF EXISTS "dim_station";
DROP TABLE IF EXISTS "dim_weather";
DROP TABLE IF EXISTS "dim_datetime";

CREATE TABLE "fact_journey" (
  "rental_id" int PRIMARY KEY,
  "bike_id" int,
  "start_date" timestamp,
  "end_date" timestamp,
  "start_station" int,
  "end_station" int,
  "weather_date" date
);

CREATE TABLE "dim_station" (
  "station_id" int PRIMARY KEY,
  "station_name" varchar,
  "longitude" double precision,
  "latitude" double precision,
  "easting" double precision,
  "northing" double precision
);

CREATE TABLE "dim_weather" (
  "weather_date" date PRIMARY KEY,
  "feelslike" double precision,
  "feelslikemax" double precision,
  "feelslikemin" double precision,
  "humidity" double precision,
  "moonphase" double precision,
  "precip" double precision,
  "pressure" double precision,
  "solarenergy" double precision,
  "solarradiation" double precision,
  "sunrise" varchar,
  "sunset" varchar,
  "temp" double precision,
  "tempmax" double precision,
  "tempmin" double precision,
  "tzoffset" double precision,
  "uvindex" double precision,
  "visibility" double precision,
  "winddir" double precision,
  "windgust" double precision,
  "windspeed" double precision
);

CREATE TABLE "dim_datetime" (
  "datetime_id" timestamp PRIMARY KEY,
  "second" int,
  "minute" int,
  "hour" int,
  "day" int,
  "month" int,
  "week_day" int,
  "year" int
);

ALTER TABLE "fact_journey" ADD FOREIGN KEY ("start_date") REFERENCES "dim_datetime" ("datetime_id");

ALTER TABLE "fact_journey" ADD FOREIGN KEY ("end_date") REFERENCES "dim_datetime" ("datetime_id");

ALTER TABLE "fact_journey" ADD FOREIGN KEY ("start_station") REFERENCES "dim_station" ("station_id");

ALTER TABLE "fact_journey" ADD FOREIGN KEY ("end_station") REFERENCES "dim_station" ("station_id");

ALTER TABLE "fact_journey" ADD FOREIGN KEY ("weather_date") REFERENCES "dim_weather" ("weather_date");