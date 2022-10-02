import csv
import cv2
from datetime import datetime,timedelta
import numpy as np
import random
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
		# Configure the video
		_self.curfile = _self.name + '_' + datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f%z') + '.avi'
		_self.width = res[1]
		_self.height = res[0]
		# If you want a time-lapse, increase 1.0 higher, or just set this to 30.0
		playback_framerate = 1.0/_self.period
		# Default OpenCV does not include h264 due to licensing issues
		_self.writer = cv2.VideoWriter(_self.curfile,
			cv2.VideoWriter_fourcc(*'XVID'),
			playback_framerate, (_self.width, _self.height))
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

print('Loading source configuration file')
with open('sources.csv', newline='') as f:
	reader = csv.DictReader(f)
	for row in reader:
		print('\t',row)
		newSource = sourceState()
		newSource.name = row['name']
		newSource.period = float(row['period'])
		newSource.url = row['url']

		sources[row['name']] = newSource

def processFrame(source):
	r = requests.get(source.url)
	if r.status_code == 200:
		try:
			frame = cv2.imdecode( np.asarray(bytearray(r.content),dtype="uint8"), cv2.IMREAD_COLOR )
			print("Frame on stream", source.name, frame.shape)
			source.write(frame)
		except Exception as er:
			print("Error on stream", source.name, frame.shape, er)
	else:
		print("Error on stream", source.name, "HTTP" + str(r.status_code) )
		source.release()
	scheduler.add_job(processFrame, run_date=datetime.now() + timedelta(seconds = source.period), args=[source])

print('Scheduling initial requests')
scheduler = BlockingScheduler()
for source in sources.values():
	run_date = datetime.now() + timedelta(seconds = random.uniform(0,source.period))
	scheduler.add_job(processFrame, run_date=run_date, args=[source])
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
