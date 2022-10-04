from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from pytz import utc
from jobMgmt import *


def error_listener(event):
	"""error handler"""
	print(f'Job {event.job_id} raised {event.exception.__class__.__name__}')

	"""set job status to error"""
	set_job_status(event.job_id, 'error')


def missed_job_listener(event):
	if event.job_id == "scheduler-job-id":
		return

	print(f'job {event.job_id} wurde verpasst')


"""def execution_listener(events):
	print(len(events))"""


"""create Scheduler"""
scheduler = BackgroundScheduler(timezone=utc)
"""start every x time interval to process jobs"""
scheduler.add_job(lambda: schedule_jobs(scheduler), 'interval', seconds=5, next_run_time=datetime.utcnow(), id='scheduler-job-id')
"""Listener for specified events"""
scheduler.add_listener(error_listener, EVENT_JOB_ERROR)
"""scheduler.add_listener(execution_listener, EVENT_JOB_EXECUTED)"""
scheduler.add_listener(missed_job_listener, EVENT_JOB_MISSED)
"""start scheduler"""
scheduler.start()

input()
