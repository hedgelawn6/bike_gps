import csv
import sqlite3
import sys
from datetime import datetime as dt
from dateutil.parser import parse
from geopy.distance import vincenty, great_circle
from geopy.geocoders import Nominatim

#Returns the distance between 2 coords
def get_dist(a, b):
    '''Takes 2 arguments - both lat, lon tuples.
    Returns km distance between 2 lat/lon coords using geopy great_circle'''
    d =  great_circle(a, b).kilometers
    return d

#A tidy place to keep sql queries
def get_sql():
	'''Returns a dict of sql queries'''
	
	sql_d = {
	'create_db':
	'''CREATE TABLE IF NOT EXISTS Event
	(
		id INTEGER PRIMARY KEY,
		Time TEXT UNIQUE NOT NULL, 
		Lat TEXT, 
		Lon TEXT, 
		Alt TEXT, 
		Speed TEXT, 
		Heading TEXT, 
                Journey_id INTEGER, 
                FOREIGN KEY(Journey_id) REFERENCES Journey(id)
	);
	CREATE TABLE IF NOT EXISTS Journey
	(
		id INTEGER PRIMARY KEY, 
		Journey_id TEXT UNIQUE NOT NULL
	);
	CREATE TABLE IF NOT EXISTS Location
	(
		id INTEGER PRIMARY KEY,
		Lat TEXT, 
		Lon TEXT,
		Name TEXT, 
		Type TEXT, 
		Label TEXT
	);
	CREATE TABLE IF NOT EXISTS Stop
	(
		id INTEGER PRIMARY KEY,
		Start_time TEXT UNIQUE,
		End_time TEXT UNIQUE,
		Location_id INTEGER,
		First_event INTEGER, 
		Last_event INTEGER,
		FOREIGN KEY(Location_id) REFERENCES Location(id)
		FOREIGN KEY(First_event) REFERENCES Event(id)
		FOREIGN KEY(Last_event) REFERENCES Event(id)
		
	);
	CREATE TABLE IF NOT EXISTS SubJourney
	(
		id INTEGER PRIMARY KEY,
		Start_time TEXT UNIQUE,
		End_time TEXT UNIQUE,
		Start_location INTEGER,
		End_location INTEGER,
		First_event INTEGER, 
		Last_event INTEGER,
		FOREIGN KEY(Start_location) REFERENCES Location(id)
		FOREIGN KEY(End_location) REFERENCES Location(id)
		FOREIGN KEY(First_event) REFERENCES Event(id)
		FOREIGN KEY(Last_event) REFERENCES Event(id)
		
	);
	''', 
        'pop_journey':
        '''INSERT OR IGNORE INTO Journey(Journey_id) VALUES(?)''',
        'get_journeys':
        '''SELECT Journey_id, id FROM Journey''',
        'pop_event':
	'''
        INSERT OR IGNORE INTO Event(Time, Lat, Lon, Alt, Speed, Heading, Journey_id) 
	VALUES(?, ?, ?, ?, ?, ?, ?)
	''', 
        'get_events':
        '''SELECT id, Time, Lat, Lon, Journey_id FROM Event'''
	
	
	}
	return sql_d  
	
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
	
#Populates db
def pop_db(src_csv, con):
	'''Takes 2 arguments - db name'''
	
	cur = con.cursor()
	sql_d = get_sql()

        #Populates the Journey table
	with open(src_csv, 'r') as f:
		csv_r = csv.reader(f)
		for row in csv_r:
                    cur.execute(sql_d['pop_journey'], (row[-1],))
		    
        #Populates the Event table
        with open(src_csv, 'r') as f:
            csv_r = csv.reader(f)
            cur.execute(sql_d['get_journeys'])
            j_d = {k:v for k, v in cur.fetchall()}
            for row in csv_r:
                    cur.execute(sql_d['pop_event'], tuple(row[:-1] + [j_d[row[-1]]]))

        #Populates the Stop table
        cur.execute(sql_d['get_events'])
        ev_l = cur.fetchall()
        s_count, j_count = 0, 0
        s_d, j_d = {}, {}
        j_l = []
        stop_loc = (float(ev_l[0][2]), float(ev_l[0][3])) 
        stop_time = parse(ev_l[0][1])

        t_thr = 30      #Threshols time - 30s
        d_thr = 0.005   #Threshold distance - 5m
        
        for ev in ev_l:
            coord = (float(ev[2]), float(ev[3]))
            time = parse(ev[1], dayfirst=True) 
            if ev[4] not in journey_l:
                stop_d[stop_count] = ev
                journey_l.append(ev[4])
            elif get_dist(stop, coord) < thr:
                stop_d[stop_count].append(ev)


        print '::stop_d:\n', stop_d
		
	con.commit()
	cur.close()
	con.close()
	return
	
#Creates and populates db
def make_db(src_csv='Session.csv', db_name='Session_db.db'):
	'''Takes 2 arguments:
	src_csv - a csv file of gps events - defaults to Session.csv
	db_name - the sqlite db it creates - defaults to Session_db.db
	Takes the arguments from the cl in that order.'''
	
        args = sys.argv
        if len(args) > 2:
            src_csv, db_name = args[1], args[2]
        con = init_db(db_name)
        pop_db(src_csv, con)
	return

make_db()
