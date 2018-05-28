import shapefile
import sqlite3
from sql_queries import get_sql

#Maps Events
def map_events(cur, sql_d):
    '''Creates a shapefile with all of the events in db'''

    print '::Events shapefile...'
    cur.execute("SELECT Journey_ID FROM Event GROUP BY Journey_ID")
    journeys = [r[0] for r in cur.fetchall()]

    for j in journeys:
        cur.execute(sql_d['get_journey_events'].format(j=j))
        hdr = [d[0] for d in cur.description]
        ev_l = [{k:v for k, v in zip(hdr, row)} for row in cur.fetchall()]

        f_shp = shapefile.Writer(shapefile.POINT)
        f_shp.autoBalance = 1
        f_shp.field('Time', 'C')

        for ev in ev_l:
            ev_id = ev['id']
            time = ev['Time']
            lat = ev['Lat']
            lon = ev['Lon']
            f_shp.point(float(lon), float(lat))
            f_shp.record(time, ev_id)

        f_shp.save('{j}_events'.format(j=j))

#Maps Stops
def map_stops(cur, sql_d):
    '''Takes 1 argument - cur - a cursor to an open sqlite db connection.
    Creates a shapefile for the stop'''

    print '::Stops shapefile...'
    cur.execute(sql_d['get_stops'])
    hdr = [d[0] for d in cur.description]
    stop_l = [{k:v for k, v in zip(hdr, row)} for row in cur.fetchall()]

    f_shp = shapefile.Writer(shapefile.POINT)
    f_shp.autoBalance = 1
    f_shp.field('Time', 'C')

    for stop in stop_l:
        stop_id = stop['id']
        start_time = stop['Start_time']
        lat = stop['Lat']
        lon = stop['Lon']
        dur = stop['Duration']
        name = stop['Loc_name']
        if lat != None and lon != None:
            f_shp.point(float(lon), float(lat))
            f_shp.record(start_time, dur, name, stop_id)

    f_shp.save('02_stops')

#Runs all of the mapping functions
def map_db(db='Session_db.db'):
    '''Takes 1 argument - db - path to sqlite db.
    Saves various shapefiles from it.'''

    con = sqlite3.connect(db)
    cur = con.cursor()
    sql_d = get_sql()

    map_events(cur, sql_d)
    map_stops(cur, sql_d)

    cur.close()
    con.close()
