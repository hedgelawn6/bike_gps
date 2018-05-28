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
            CHECK (Lat <> 'n/a')
    );
    CREATE TABLE IF NOT EXISTS Journey
    (
            id INTEGER PRIMARY KEY, 
            Journey_id INTEGER UNIQUE NOT NULL, 
            First_ev INTEGER, 
            Last_ev INTEGER, 
            First_loc INTEGER, 
            Last_loc INTEGER,
            Duration TEXT,
            FOREIGN KEY(First_ev) REFERENCES Event(id)
            FOREIGN KEY(Last_ev) REFERENCES Event(id)
            FOREIGN KEY(First_loc) REFERENCES Location(id)
            FOREIGN KEY(Last_loc) REFERENCES Location(id)

    );
    CREATE TABLE IF NOT EXISTS Location
    (
            id INTEGER PRIMARY KEY,
            Lat TEXT, 
            Lon TEXT,
            Name TEXT, 
            Type TEXT, 
            Label TEXT, 
            UNIQUE(Lat, Lon)
    );
    CREATE TABLE IF NOT EXISTS Stop
    (
            id INTEGER PRIMARY KEY,
            Journey_ID INTEGER,
            First_ev INTEGER, 
            Last_ev INTEGER,
            Loc_ID INTEGER,
            Duration TEXT,
            FOREIGN KEY(Journey_ID) REFERENCES Journey(id)
            FOREIGN KEY(First_ev) REFERENCES Event(id)
            FOREIGN KEY(Last_ev) REFERENCES Event(id)
            FOREIGN KEY(Loc_id) REFERENCES Location(id)
    );
    CREATE TABLE IF NOT EXISTS SubJourney
    (
            id INTEGER PRIMARY KEY,
            Start_time TEXT UNIQUE,
            End_time TEXT UNIQUE,
            Start_loc INTEGER,
            End_loc INTEGER,
            First_ev INTEGER, 
            Last_ev INTEGER,
            FOREIGN KEY(Start_loc) REFERENCES Location(id)
            FOREIGN KEY(End_loc) REFERENCES Location(id)
            FOREIGN KEY(First_ev) REFERENCES Event(id)
            FOREIGN KEY(Last_ev) REFERENCES Event(id)
            
    );
    ''', 
    'pop_journey':
    '''INSERT OR IGNORE INTO Journey(Journey_ID, First_ev, Last_ev, First_loc, Last_loc) 
    VALUES(?, ?, ?, ?, ?)''',
    'get_journeys':
    '''SELECT Journey_id, id FROM Journey''',
    'pop_event':
    '''
    INSERT OR IGNORE INTO Event(Time, Lat, Lon, Alt, Speed, Heading) 
    VALUES(?, ?, ?, ?, ?, ?)
    ''', 
    'get_events':
    "SELECT id, Time, Lat, Lon FROM Event", 
    'get_journey_events':
    "SELECT id, Time, Lat, Lon FROM Event WHERE Journey_ID = '{j}'", 
    'get_new_events':
    '''SELECT id, Time, Lat, Lon FROM Event
    WHERE Journey_ID IS NULL''', 
    'get_locs':"SELECT id, Lat, Lon FROM Location", 
    'update_event':"UPDATE Event SET Journey_ID = '{j}' WHERE id = '{i}'", 
    'pop_stops':
    "INSERT OR IGNORE INTO Stop(Journey_ID, First_ev, Last_ev, Loc_ID, Duration) VALUES(?, ?, ?, ?, ?)", 
    'pop_locs':
    "INSERT OR IGNORE INTO Location(Lat, Lon) VALUES(?, ?)",
    'get_stops':
    '''
    SELECT Stop.id AS id, Stop.Duration AS Duration, 
    Event.Time AS Start_time, 
    Location.Lat AS Lat, Location.Lon AS Lon, Location.Name AS Loc_name
    FROM Stop
    LEFT JOIN Event ON Event.id = Stop.First_ev
    LEFT JOIN Location ON Location.id = Stop.Loc_ID
    '''
    }
    return sql_d  
