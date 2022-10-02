import csv
import cv2
from datetime import datetime,timedelta
import numpy as np
import requests
import time
from apscheduler.schedulers.blocking import BlockingScheduler

class sourceState:
	name = None
	period = None
	url = None
	width = None
	height = None
	curfile = None
	writer = None
	def open(_self,res):
		_self.curfile = _self.name + '_' + datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f%z') + '.avi'
		_self.width = res[1]
		_self.height = res[0]
		_self.writer = cv2.VideoWriter(_self.curfile,
			cv2.VideoWriter_fourcc(*'MJPG'),
			1.0/_self.period, (_self.width, _self.height))
	def release(_self):
		if _self.writer is not None:
			_self.writer.release()
			_self.writer = None
		_self.curfile = None
		_self.width = None
		_self.height = None
	def write(_self,frame):
		if _self.writer is None:
			_self.open(frame.shape)
		elif _self.width != frame.shape[1] or _self.height != frame.shape[0]:
			_self.release()
			_self.open(frame.shape)
		_self.writer.write(frame)

sources = dict()

with open('sources.csv', newline='') as f:
	reader = csv.DictReader(f)
	for row in reader:
		print(row)
		newSource = sourceState()
		newSource.name = row['name']
		newSource.period = float(row['period'])
		newSource.url = row['url']

		sources[row['name']] = newSource

def processFrame(source):
	r = requests.get(source.url)
	print(r.status_code)
	if r.status_code == 200:
		frame = cv2.imdecode( np.asarray(bytearray(r.content),dtype="uint8"), cv2.IMREAD_COLOR )

		print(frame.shape)
		source.write(frame)
	else:
		source.release()
	scheduler.add_job(processFrame, run_date=datetime.now() + timedelta(seconds = source.period), args=[source])

scheduler = BlockingScheduler()
for source in sources.values():
	scheduler.add_job(processFrame, args=[source])
scheduler.start()

while(True):
	for source in sources.values():
		print('sleeping')
		time.sleep(source.period)
		print('sleep done')

# When everything done, release 
# the video capture and video 
# write objects
result.release()

# Closes all the frames
cv2.destroyAllWindows()
   
print("The video was successfully saved")
