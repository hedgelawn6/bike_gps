import make_db as mdb
import make_shapefile as msf
import geo_db as gdb

def process_csv():
    mdb.make_db()
    gdb.rev_geocode_locs()
    msf.map_db()

process_csv()



