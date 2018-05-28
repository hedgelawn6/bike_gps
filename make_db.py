import csv
import sqlite3
import sys
import datetime as dt
from dateutil.parser import parse
from geopy.distance import vincenty, great_circle
from sql_queries import get_sql

#Returns the distance between 2 coords
def get_dist(a, b):
    '''Takes 2 arguments - both lat, lon tuples.
    Returns km distance between 2 lat/lon coords using geopy great_circle'''
    d =  great_circle(a, b).kilometers
    return d
	
#Creates db
def init_db(db_name):
    '''Takes 1 argument - db name - a file path string
    Returns an open connection to a sqlite db'''
    
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    cur.executescript(get_sql()['create_db'])
    con.commit()
    cur.close()
    return con
	
#Populates event table
def pop_event(src_csv, con):
    '''Takes 2 arguments - src_csv - a path to a csv file of GPS events
    and con - an open connection to a sqlite db.
    Populates the Event table from the source file.
    Returns the connection, con.'''
    
    print '::Event table...'
    cur = con.cursor()
    sql_d = get_sql()
    with open(src_csv, 'r') as f:
        csv_r = csv.reader(f)
        for row in csv_r:
            cur.execute(sql_d['pop_event'], tuple(row))

    con.commit()
    cur.close()

#Populates the journey table
def pop_journey(con, t_thr, d_thr):
    '''Takes 3 arguments - con - an open connection to a sqlite db
    t_thr/d_thr - time and dist thresholds.
    Identifies Journeys. Populates the Journey table and updates Journey_IDs into the Event table.
    Returns 2 dicts - j_d - dict of journeys and loc_d - dict of locations.'''
    
    print '::Journey table...'
    cur = con.cursor()
    sql_d = get_sql()

    #Gets a list of event dicts from the db
    cur.execute(sql_d['get_new_events'])
    hdr = [d[0] for d in cur.description]
    ev_l = [{k:v for k, v in zip(hdr, row)} for row in cur.fetchall()]
    cur.execute("SELECT MAX(id) FROM Journey")
    last_j = cur.fetchall()[0][0] if cur.fetchall()[0][0] != None else 0
    next_j = last_j + 1
    j_d = {next_j:[]}

    #Gets a dict of locations from the db
    cur.execute("SELECT MAX(id) FROM Location")
    next_loc = (cur.fetchall()[0][0] if cur.fetchall()[0][0] != None else 0) + 1
    cur.execute(sql_d['get_locs'])
    loc_d = {row[0]:(float(row[1]), float(row[2])) for row in cur.fetchall()} 

    #Starting parameters
    start_t = parse(ev_l[0]['Time'], dayfirst=True)
    start_p = (float(ev_l[0]['Lat']), float(ev_l[0]['Lon'])) 
    
    #Iterates through the events and separates them into journeys
    for ev in ev_l:
        p = (float(ev['Lat']), float(ev['Lon']))
        t = parse(ev['Time'])
        if dt.timedelta(seconds=t_thr) < t - start_t:
            next_j += 1
            j_d[next_j] = [ev]
        else:
            j_d[next_j].append(ev)
        start_t = t

    #Populates the Journey table and updates the Event table from j_d
    for j in j_d:
        first_ev = j_d[j][0]['id']
        last_ev = j_d[j][-1]['id']
        first_p = (float(j_d[j][0]['Lat']), float(j_d[j][0]['Lon']))
        last_p = (float(j_d[j][-1]['Lat']), float(j_d[j][-1]['Lon']))
        first_t = parse(j_d[j][0]['Time'], dayfirst=True)
        last_t = parse(j_d[j][-1]['Time'], dayfirst=True)
        first_loc, last_loc = None, None
        
        #First location
        for loc in loc_d:
            if get_dist(first_p, loc_d[loc]) < d_thr:
                first_loc = loc
                break
        if first_loc == None:
            loc_d[next_loc] = first_p
            first_loc = next_loc
            next_loc += 1
        
        #Last location
        for loc in loc_d:
            if get_dist(last_p, loc_d[loc]) < d_thr:
                last_loc = loc
                break
        if last_loc == None:
            loc_d[next_loc] = last_p
            last_loc = next_loc
            next_loc += 1
    
        #Adds the data to the Event and Journey tables
        cur.execute(sql_d['pop_journey'], (j, first_ev, last_ev, first_loc, last_loc))
        for row in j_d[j]:
            cur.execute(sql_d['update_event'].format(j=j, i=row['id']))
            
    con.commit()
    cur.close()
    return j_d, loc_d
	
#Adds stops to the db
def pop_stops(con, j_d, loc_d, t_thr, d_thr):
    '''Takes 5 arguments - con - an open connection to the sqlite db,
    j_d - dict of journeys, loc_d - dict of locations, 
    t_thr and d_thr - time and distance threshold values.
    For each journey identifies stops.
    Checks whether this stop is at new location, and adds on if so.
    Populates the stops table.
    Returns an updated dict of locations - loc_d.'''
    
    print '::Stop table...'
    sql_d = get_sql()
    cur = con.cursor()
    stop_l = []
    next_loc = max(loc_d) + 1

    #Iterates through the events in each journey identifying stops
    for j in j_d:
        first_p = (float(j_d[j][0]['Lat']), float(j_d[j][0]['Lon']))
        first_t = parse(j_d[j][0]['Time'], dayfirst=True)
        stop = []
        stop_d = {}

        for ev in j_d[j]:
            t = parse(ev['Time'])
            p = (float(ev['Lat']), float(ev['Lon']))
            if get_dist(p, first_p) < d_thr:
                stop.append(ev)
            else:
                if len(stop) > 0:
                    dur = parse(stop[-1]['Time'], dayfirst=True) - parse(stop[0]['Time'], dayfirst=True)
                    if dt.timedelta(seconds=t_thr) < dur:
                        stop_l.append(  {   'Journey_ID':j, 
                                            'First_ev':stop[0]['id'], 
                                            'Last_ev':stop[-1]['id'], 
                                            'Loc':(float(stop[0]['Lat']), float(stop[0]['Lon'])), 
                                            'Dur':str(dur)
                                        })
                first_p = p
                stop = []

    #Gets locations for the stops. Updates loc_d with new locations.
    for s in stop_l:
        for l in loc_d:
            if get_dist(s['Loc'], loc_d[l]) < d_thr:
                s['Loc'] = loc_d[l]
                break
    for s in stop_l:
        if type(s['Loc']) != int:
            loc_d[next_loc] = s['Loc']
            s['Loc'] = next_loc
            next_loc += 1

    #Adds the stops to the stop table
    for s in stop_l:
        cur.execute(sql_d['pop_stops'], (s['Journey_ID'], s['First_ev'], s['Last_ev'], s['Loc'], s['Dur']))

    con.commit()
    cur.close()
    return loc_d

#Adds locations to the db
def pop_locs(con, loc_d):
    '''Takes 2 arguments - con - open connection to sqlite db, loc_d - dict of locs, output of pop_stops()'''

    print '::Location table...'
    cur = con.cursor()
    sql_d = get_sql()
    for loc in loc_d:
        cur.execute(sql_d['pop_locs'], loc_d[loc])
    con.commit()
    cur.close
    return

#Creates and populates db
def make_db(src_csv='Session.csv', db_name='Session_db.db'):
    '''Takes 2 arguments:
    src_csv - a csv file of gps events - defaults to Session.csv
    db_name - the sqlite db it creates - defaults to Session_db.db
    Takes the arguments from the cl in that order.'''
	
    #Threshold parameters
    t_thr = 30      #Threshols time - 30s
    d_thr = 0.005   #Threshold distance - 5m
    
    con = init_db(db_name)
    pop_event(src_csv, con)
    j_d, loc_d = pop_journey(con, t_thr, d_thr)
    pop_stops(con, j_d, loc_d, t_thr, d_thr)
    pop_locs(con, loc_d)
    return
