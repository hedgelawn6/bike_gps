import gps
import csv
from datetime import datetime as dt

#Dumps the output from the GPS sensor into a csv file
def gps_capture():
	'''Dumps the output from the GPS sensor into a csv file.
	Saves the event log csv file one line at a 
	time so that it still works if the power is cut.'''
	
	j_id = str(dt.now())
	session = gps.gps("localhost", "2947")
	session.stream (gps.WATCH_ENABLE | gps.WATCH_NEWSTYLE)

	while True:
		try:
			report = session.next()
			if report['class'] == 'TPV':
				if hasattr(report, 'time'):
					print 'Lat:', report.lat
					print 'Lon:', report.lon
					row =	[
								report.time, 
								report.lat, 
								report.lon, 
								report.alt, 
								report.speed,
								report.track,
								j_id
							]
					with open('Session.csv', 'a+') as f:
						csv_w = csv.writer(f)
						csv_w.writerow(row)
		except KeyboardInterrupt:
			quit()
gps_capture()
