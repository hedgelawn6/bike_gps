import csv
import sqlite3
import sys

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
	);
	CREATE TABLE IF NOT EXISTS Journey
	(
		id INTEGER PRIMARY KEY, 
		Journey_id TEXT UNIQUE NOT NULL,
		First_event INTEGER, 
		Last_event INTEGER,
		FOREIGN KEY(First_event) REFERENCES Event(id)
		FOREIGN KEY(Last_event) REFERENCES Event(id)
	);
	CREATE TABLE IF NOT EXISTS Location
	(
		id INTEGER PRIMARY KEY
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
		End_time TEXT UNIQUE
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
		End_time TEXT UNIQUE
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
	'pop_event':
	'''INSERT OR IGNORE INTO Event(Time, Lat, Lon, Alt, Speed, Heading) 
	VALUES(?, ?, ?, ?, ?, ?)
	'''
	
	
	}
	return sql_d
	
#Creates db
def init_db(db_name):
	'''Takes 1 argument - db name - a file path string
	Returns an open connection to a sqlite db'''
	
	con = sqlite3.connect(db_name)
	cur = con.cursor()
	cur.executescript(get_sql()['creat_db'])
	con.commit()
	cur.close()
	return con
	
#Populates db
def pop_db(src_csv, con):
	'''Takes 2 arguments - db name'''
	
	cur = con.cursor()
	sql_d = get_sql()
	with open(src_csv 'r') as f:
		csv_r = csv.reader(f)
		for row in csv_r:
			cur.execute(sql_d['pop_event'], tuple(row[:-1]))
		
	con.commit()
	cur.close()
	con.close()
	
	
	
	return
	
#Creates and populates db
def make_db(src_csv='Session.csv', db_name='Session_db.db'):
	'''Takes 2 arguments:
	src_csv - a csv file of gps events - defaults to Session.csv
	db_name - the sqlite db it creates - defaults to Session_db.db
	Takes the arguments from the cli in that order.'''
	
	return
