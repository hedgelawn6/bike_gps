# bike_gps
Raspberry Pi GPS tracker for logging motorcycle journeys.

get_sensor_data.py runs on Rpi under bike seat.

bike_gps.py imports the othre modules.

Loads the csv of GPS events into a SQLite db, identifies journeys and stops.

Reverse geocodes locations.

Creates a shapefile for each journey.
