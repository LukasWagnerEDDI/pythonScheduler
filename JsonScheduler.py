from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from pytz import utc
import jobMgmt
from datetime import datetime

def scheduleJobs():
	jobMgmt.schedule_jobs(scheduler)

def error_listener(event):
	"""error handler"""
	print(f'Job {event.job_id} raised {event.exception.__class__.__name__}')

	"""set job status to error"""
<<<<<<< HEAD
	#jobMgmt.set_job_status(event.job_id, 'error')
=======
	jobMgmt.set_job_status(event.job_id, 'error')
>>>>>>> dcc0d62b94481665770b0c44526a3d83a07b5115


def missed_job_listener(event):
	if event.job_id == "scheduler-job-id":
		return

	print(f'job {event.job_id} was missed')
	if scheduler.get_job(job_id=event.job_id):
		job = scheduler.get_job(job_id=event.job_id)
		print(f'next execution: {job.next_run_time}')


"""def execution_listener(events):
	print(len(events))"""
if __name__ == '__main__':

	jobStores = {
		'default': RedisJobStore(host='127.0.0.1', port=6379)
	}

	"""create Scheduler"""
	global scheduler
	scheduler = BackgroundScheduler(jobstores=jobStores, timezone=utc)
	"""start every x time interval to process jobs"""
	job = scheduler.add_job(scheduleJobs, 'interval',replace_existing=True, seconds=5, next_run_time=datetime.utcnow(), id='scheduler-job-id')
	"""Listener for specified events"""
	scheduler.add_listener(error_listener, EVENT_JOB_ERROR)
	"""scheduler.add_listener(execution_listener, EVENT_JOB_EXECUTED)"""
	scheduler.add_listener(missed_job_listener, EVENT_JOB_MISSED)
	"""start scheduler"""
	scheduler.start()
	

	input()
