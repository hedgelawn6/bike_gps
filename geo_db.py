import csv
import geocoder
import sqlite3

#Gets API keys
def get_keys():
    '''Returns a dictionary of API keys from the keys.csv file'''

    with open("keys.csv", 'r') as f:
        csv_r = csv.reader(f)
        key_d = {r[0]:r[1] for r in csv_r}

    return key_d

#Updates location table with place names
def rev_geocode_locs(db='Session_db.db'):
    '''Takes 1 argument - db - path to sqlite db.
    Updates the Location table with reverse geocoded location names using MapQuest API'''

    k = get_keys()['MapQuest']
    con = sqlite3.connect(db)
    cur = con.cursor()
    get_locs_sql = "SELECT id, Lat, Lon FROM Location"
    pop_locs_sql = '''UPDATE Location SET Name = "{s}, {c}, {p}" WHERE id = {i}'''
    cur.execute(get_locs_sql)
    co_ords = cur.fetchall()

    for c in co_ords:
        g = geocoder.mapquest([c[1], c[2]], method='reverse', key=k)
        geo = g.json
        street = 'unid' if 'street' not in geo else geo['street']
        city = 'unid' if 'city' not in geo else geo['city']
        postcode = 'unid' if 'postcode' not in geo else geo['postcode']
        cur.execute(pop_locs_sql.format(s=street, c=city, p=postcode, i=c[0]))

    cur.close()
    con.commit()
    con.close()
