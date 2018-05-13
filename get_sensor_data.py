import gps
import csv
import os
import sys
from datetime import datetime as dt

#Dumps the output from the GPS sensor into a csv file
def gps_capture(x=10):
	'''Takes 1 argument - number of GPS fixes to quit after - defaults 10.
	Dumps the output from the GPS sensor into a csv file.
	Restarts the GPS, and saves the event log csv file one line at a 
	time so that it still works if the power is cut.'''
	
	args = sys.argv
	x = int(args[1])
	
	os.system("sudo killall gpsd")
	os.system("sudo gpsd /dev/ttyUSB0 -F /var/run/gpsd.sock")
		
	hdr = ['Time', 'Lat', 'Lon', 'Alt', 'Speed', 'Heading', 'Journey_ID']
	j_id = str(dt.now())
	
	session = gps.gps("localhost", "2947")
	session.stream (gps.WATCH_ENABLE | gps.WATCH_NEWSTYLE)

	row_count = 0

	while row_count < x:
		try:
			report = session.next()
			print report
				
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
					
					row_count += 1
	
		except KeyboardInterrupt:
			quit()
			
gps_capture()
	
	
