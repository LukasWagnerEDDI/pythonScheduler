import logging
import redis
import json
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from pytz import utc
import jobMgmt
import redisMgmt
from datetime import datetime


def scheduleJobs():
	jobMgmt.schedule_jobs(scheduler)


def error_listener(event):
	"""error handler"""
	print(f'+++++++++++++++++++++++++++ \n\n{str(event)}\n\n')
	print(f'Job {event.job_id} raised {event.exception.__class__.__name__}\n\n+++++++++++++++++++++++++++\n')

	"""set job status to error"""
	if event.job_id == "scheduler-job-id":
		return

	jobMgmt.set_job_status(event.job_id, 'error')


def missed_job_listener(event):
	if event.job_id == "scheduler-job-id":
		return

	print(f'job {event.job_id} was missed\n')
	if scheduler.get_job(job_id=event.job_id):
		job = scheduler.get_job(job_id=event.job_id)
		print(f'next execution: {job.next_run_time}\n')

	jobMgmt.set_job_status(event.job_id, 'missed')


"""def execution_listener(events):
	print(len(events))"""

if __name__ == '__main__':
	logging.basicConfig()
	# logging.DEBUG/logging.INFO
	#logging.getLogger('apscheduler').setLevel(logging.DEBUG)

	jobStores = {
		'default': RedisJobStore(host='127.0.0.1', port=6379)
	}

	"""create Scheduler"""
	mainJobId = 'scheduler-job-id'
	global scheduler
	scheduler = BackgroundScheduler(jobstores=jobStores, timezone=utc)
	"""start every x time interval to process jobs"""
	#RedisConnect = redis.StrictRedis(host='localhost', port=6379, db=0)
	if not redisMgmt.get_job(mainJobId):
		job = scheduler.add_job(scheduleJobs,'interval',seconds=5,start_date='2010-10-10 09:30:00',replace_existing=True,next_run_time=datetime.utcnow(),id=mainJobId)

		jsonString = json.loads("{" + f'"id": "{str(mainJobId)}", "version": "0", "creation_time": "{str(datetime.utcnow())}", "last_execution_time": "1970-01-01 00:00:00", "next_execution_time": "{str(job.next_run_time)}", "error_message": ""' + "}")
		redisMgmt.set_job_info(jsonString)

	"""Listener for specified events"""
	scheduler.add_listener(error_listener, EVENT_JOB_ERROR)
	"""scheduler.add_listener(execution_listener, EVENT_JOB_EXECUTED)"""
	scheduler.add_listener(missed_job_listener, EVENT_JOB_MISSED)
	"""start scheduler"""
	scheduler.start()
	job = scheduler.get_job(mainJobId)
	job.modify(next_run_time=datetime.utcnow())

	input()
