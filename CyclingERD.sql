CREATE TABLE "fact_journey" (
  "rental_id" int PRIMARY KEY,
  "bike_id" int,
  "start_date" timestamp,
  "end_date" timestamp,
  "start_station_id" int,
  "end_station_id" int,
  "weather_id" datetime
);

CREATE TABLE "dim_station" (
  "station_id" int PRIMARY KEY,
  "station_name" varchar,
  "longitude" long,
  "latitude" long,
  "easting" real,
  "northing" real
);

CREATE TABLE "dim_weather" (
  "weather_datetime" datetime PRIMARY KEY,
  "tempmax" real,
  "tempmin" real,
  "temp" real,
  "feelslikemax" real,
  "feelslikemin" real,
  "feelslike" real,
  "humidity" real,
  "precip" real,
  "precipprob" varchar,
  "preciptype" varchar,
  "snow" varchar,
  "snowdepth" real,
  "windgust" real,
  "windspeed" real,
  "winddir" real,
  "pressure" real,
  "visibility" real,
  "solarradiation" real,
  "solarenergy" real,
  "uvindex" real,
  "sunrise" varchar,
  "sunset" varchar,
  "moonphase" real,
  "conditions" varchar,
  "tzoffset" real
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

ALTER TABLE "fact_journey" ADD FOREIGN KEY ("start_station_id") REFERENCES "dim_station" ("station_id");

ALTER TABLE "fact_journey" ADD FOREIGN KEY ("end_station_id") REFERENCES "dim_station" ("station_id");

ALTER TABLE "dim_weather" ADD FOREIGN KEY ("weather_datetime") REFERENCES "fact_journey" ("weather_id");
