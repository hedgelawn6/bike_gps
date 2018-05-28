import gps
import csv
import os
from datetime import datetime as dt

#Restarts gpsd
def restart_gpsd(x):
	'''Takes 1 argument - an integer number of times to restart
	the gpsd process.'''
	
	for i in range(x):	
		print "reset", i
		os.system("sudo killall gpsd")
		os.system("sudo gpsd /dev/ttyUSB0 -F /var/run/gpsd.sock")
		os.system("sleep 5")
	
	print "gpsd reset"

#Dumps the output from the GPS sensor into a csv file
def gps_capture():
	'''Dumps the output from the GPS sensor into a csv file.
	Saves the event log csv file one line at a 
	time so that it still works if the power is cut.'''
	
	restart_gpsd(3)
	
	j_id = str(dt.now())
	session = gps.gps("localhost", "2947")
	session.stream (gps.WATCH_ENABLE | gps.WATCH_NEWSTYLE)

	while True:
		try:
			report = session.next()
			if report['class'] == 'TPV':
				if hasattr(report, 'time'):
					try:
						alt = report.alt
					except:
						alt = 'n/a'
					try:
						speed = report.speed
					except:
						speed = 'n/a'
					try:
						track = report.track
					except:
						track = 'n/a'
					try:
						lat = report.lat
					except:
						lat = 'n/a'
					try:
						lon = report.lon
					except:
						lon = 'n/a'
					print 'Lat:', lat
					print 'Lon:', lon
					row =	[
								report.time, 
								lat, 
								lon, 
								alt, 
								speed,
								track,
								j_id
							]
					with open('Session.csv', 'a+') as f:
						csv_w = csv.writer(f)
						csv_w.writerow(row)
		except KeyboardInterrupt:
			quit()
gps_capture()
