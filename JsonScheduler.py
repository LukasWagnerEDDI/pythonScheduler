from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR
from pytz import utc
from datetime import datetime
import json
import dateConversion

scheduled_jobs_map = {}


def schedule_jobs(scheduler_l):
	"""retrieve jobs from jobs.json file"""
	jobs = retrieve_jobs_to_schedule()

	for job in jobs:
		"""add new job to schedule"""
		add_job_if_applicable(job, scheduler)
		"""update edited job - checking for different job version"""
		update_job_if_applicable(job, scheduler)

	print("refreshed scheduled jobs")


def retrieve_jobs_to_schedule():
	with open('jobs.json') as f:
		d = json.load(f)

	return d


def add_job_if_applicable(job, scheduler_l):
	job_id = str(job['id'])
	if (job_id not in scheduled_jobs_map):
		"""add job to job list cache"""
		scheduled_jobs_map[job_id] = job
		"""add job to schedule as cron tab"""
		scheduler.add_job(lambda: execute_job(job), CronTrigger.from_crontab(job['cron_expression'],timezone='UTC'),id=job_id)

		print("added job with id: " + str(job_id))


def update_job_if_applicable(job, scheduler_l):
	job_id = str(job['id'])
	"""job can only be updated if it already exists"""
	if (job_id not in scheduled_jobs_map):
		return

	"""check for changed version"""
	last_version = scheduled_jobs_map[job_id]['version']
	current_version = job['version']
	if (current_version != last_version):
		# scheduled_jobs_map[job_id]['version'] = current_version
		"""refresh job in cache"""
		scheduled_jobs_map[job_id] = job
		"""add refreshed job to schedule"""
		scheduler.remove_job(job_id)
		scheduler.add_job(lambda: execute_job(job), CronTrigger.from_crontab(job['cron_expression'], timezone='UTC'), id=job_id)
		print("updated job with id: " + str(job_id))


def reschedule_job(job_id, epoch_time):
	jobs = retrieve_jobs_to_schedule()
	new_cron_expression = dateConversion.convert_epoch_to_cron_expression(epoch_time)
	if jobs[job_id]["cron_expression"] != new_cron_expression:
		adjust_job_property(job_id, "cron_expression", new_cron_expression)


def execute_job(job):
	print("executing job with id: " + str(job['id']))
	print(datetime.utcnow())
	"""run specified script          * * * no parameter input yet * * * """
	if ('' != job["runScript"]):
		exec(open(job["runScript"]).read())


"""def increase_job_status(job_id):"""


def set_job_status(job_id, status):
	adjust_job_property(job_id, 'status', status)


def adjust_job_property(job_id, property_name, value):
	jobs_l = {}
	"""retrieve most recent job list"""
	jobs_l = retrieve_jobs_to_schedule()
	"""set new value and save the json"""
	jobs_l[job_id][property_name] = value
	f = open("jobs.json", "w")
	json.dump(jobs_l, f)
	f.close()


def refresh_run_timestamp(job_id):
	adjust_job_property(job_id, "")

def error_listener(event):
	"""error handler"""
	print(f'Job {event.job_id} raised {event.exception.__class__.__name__}')
	if (scheduled_jobs_map[event.job_id]["status"] == 'requested'):

		"""set job status to error"""
		set_job_status(event.job_id, 'error')


"""create Scheduler"""
scheduler = BackgroundScheduler(timezone=utc)
"""start every x time interval to process jobs"""
scheduler.add_job(lambda: schedule_jobs(scheduler), 'interval', seconds=5, next_run_time=datetime.utcnow(), id='scheduler-job-id')
"""Listener for specified events"""
scheduler.add_listener(error_listener, EVENT_JOB_ERROR)
scheduler.start()

input()