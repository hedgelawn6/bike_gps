import geocoder
import sqlite3

k = "hteEKYs3TAxzzQMsPNqxg4G80iT3XwYy"

#Updates location table with place names
def rev_geocode_locs(db='Session_db.db'):
    '''Takes 1 argument - db - path to sqlite db.
    Updates the Location table with reverse geocoded location names using MapQuest API'''

    con = sqlite3.connect(db)
    cur = con.cursor()
    get_locs_sql = "SELECT id, Lat, Lon FROM Location"
    pop_locs_sql = '''UPDATE Location SET Name = "{s}, {c}, {p}" WHERE id = {i}'''
    cur.execute(get_locs_sql)
    co_ords = cur.fetchall()

    for c in co_ords:
        g = geocoder.mapquest([c[1], c[2]], method='reverse', key=k)
        geo = g.json
        street, city, postcode = geo['street'], geo['city'], geo['postal']
        cur.execute(pop_locs_sql.format(s=street, c=city, p=postcode, i=c[0]))

    cur.close()
    con.commit()
    con.close()

rev_geocode_locs()
