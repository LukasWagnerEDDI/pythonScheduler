from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from pytz import utc
from datetime import datetime
import json
import dateConversion
import subprocess

scheduled_jobs_map = {}


def schedule_jobs(scheduler_l):
	"""retrieve jobs from jobs.json file"""
	jobs = retrieve_jobs_to_schedule()

	for job in jobs:
		"""skip finished job"""
		if job["status"] in ["finished", "error"]:
			continue

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
	if job_id not in scheduled_jobs_map:
		"""add job to job list cache"""
		scheduled_jobs_map[job_id] = job
		"""add job to schedule as cron tab"""
		scheduler.add_job(lambda: execute_job(job), CronTrigger.from_crontab(job['cron_expression'], timezone='UTC'), id=job_id)

		print("added job with id: " + str(job_id))


def update_job_if_applicable(job, scheduler_l):
	job_id = str(job['id'])
	"""job can only be updated if it already exists"""
	if job_id not in scheduled_jobs_map:
		return

	"""check for changed version"""
	last_version = scheduled_jobs_map[job_id]['version']
	current_version = job['version']
	if current_version != last_version:
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
		set_job_status(job_id, "requested")


def restart_job(job_id):
	jobs_l = retrieve_jobs_to_schedule()


def get_index_for_job_id(job_list, job_id):
	for index, job in enumerate(job_list):
		if job["id"] == job_id:
			return index

def execute_job(job):
	print("executing job with id: " + str(job['id']))
	print(datetime.utcnow())
	"""set readable run timestamp"""
	refresh_run_timestamp(job["id"])
	set_job_status(job["id"], "running")
	"""run specified script"""
	if '' != job["run_script"]:
		if job["uploaded_file"] != "":
			subprocess.call(job["run_script"] + " " + job["uploaded_file"])
		else:
			subprocess.call(job["run_script"])

	"""set finished or success status"""
	jobs_l = retrieve_jobs_to_schedule()
	job_index = get_index_for_job_id(jobs_l, job['id'])
	if jobs_l[job_index]["repeat"] == "true":
		set_job_status(job["id"], "success")
	else:
		set_job_status(job["id"], "finished")

def set_job_status(job_id, status):
	adjust_job_property(job_id, 'status', status)


def adjust_job_property(job_id, property_name, value):
	job_index = 0
	"""retrieve most recent job list"""
	jobs_l = retrieve_jobs_to_schedule()
	job_index = get_index_for_job_id(jobs_l, job_id)

	"""set new value"""
	jobs_l[job_index][property_name] = str(value)
	"""set new update timestamp"""
	jobs_l[job_index]["last_update"] = str(datetime.utcnow())
	"""increment version"""
	if property_name not in ["status", "last_execution_time"]:
		jobs_l[job_index]["version"] = int(jobs_l[job_index]["version"]) + 1

	"""save json"""
	f = open("jobs.json", "w")
	json.dump(jobs_l, f)
	f.close()

	"""refresh job in cache"""
	print(scheduled_jobs_map)
	if str(scheduled_jobs_map.get(job_id)) != str(jobs_l[job_index]):
		scheduled_jobs_map[job_id] = jobs_l[job_index]


def refresh_run_timestamp(job_id):
	adjust_job_property(job_id, "last_execution_time", datetime.utcnow())


def error_listener(event):
	"""error handler"""
	print(f'Job {event.job_id} raised {event.exception.__class__.__name__}')

	"""set job status to error"""
	set_job_status(event.job_id, 'error')


def execution_listener(event):
	if event.job_id == "scheduler-job-id":
		return


"""def missed_job_listener(event):
	reschedule_job()"""


"""create Scheduler"""
scheduler = BackgroundScheduler(timezone=utc)
"""start every x time interval to process jobs"""
scheduler.add_job(lambda: schedule_jobs(scheduler), 'interval', seconds=5, next_run_time=datetime.utcnow(), id='scheduler-job-id')
"""Listener for specified events"""
scheduler.add_listener(error_listener, EVENT_JOB_ERROR)
scheduler.add_listener(execution_listener, EVENT_JOB_EXECUTED)
scheduler.add_listener(execution_listener, EVENT_JOB_MISSED)
"""start scheduler"""
scheduler.start()

input()
