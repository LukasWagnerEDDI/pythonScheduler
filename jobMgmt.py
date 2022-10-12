import sys
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime
import json
import dateConversion
import redis

import subprocess

redisConn = redis.StrictRedis(host='localhost', port=6379, db=0)

redis_job_hash = 'apscheduler.jobs'
scheduled_jobs = {}
retrieved_jobs = {}


class Job:
	def __init__(self, job=""):
		self.job = job

	def execute(self):
		execute_job(self.job)


def schedule_jobs(scheduler_p):
	"""retrieve jobs from jobs.json file"""
	jobs = retrieve_jobs_from_redis()

	for key in jobs.keys():
		if jobs[key] == "scheduler-job-id":
			continue

		job = jobs[key]
		"""skip finished job"""
		if job["status"] in ["finished", "error", "missed"]:
			remove_job(job['id'], scheduler_p, False)
			continue

		"""add new job to schedule"""
		add_job_if_applicable(job, scheduler_p)
		"""update edited job - checking for different job version"""
		update_job_if_applicable(job, scheduler_p)

	print("refreshed scheduled jobs")


def execute_job(job):
	print("executing job with id: " + str(job['id']))
	print(datetime.utcnow())
	"""set readable run timestamp"""
	refresh_run_timestamp(job["id"])
	set_job_status(job["id"], "running")
	if '' != job["run_script"]:
		"""create list of given parameters"""
		params = ["python", job["run_script"]]
		params.extend(
			# [val for key, val in job["script_parameters"].items() if val != ""]
			[f'{key}={val}' for key, val in job['script_parameters'].items() if val != ""]
		)
		subprocess.call(params)

	"""set finished or success status"""
	jobs_l = retrieve_jobs_from_redis()
	if jobs_l[str(job['id'])]["cron_expression"] != "":
		set_job_status(job["id"], "success")
	else:
		set_job_status(job["id"], "finished")


def retrieve_jobs_from_redis():
	job_info_keys = []

	fields = redisConn.hgetall(redis_job_hash)
	keys = fields.keys()
	for key in keys:
		key = str(bytes(key).decode())
		if '.info' in key:
			job_info_keys.append(key)

	for job_info_key in job_info_keys:
		retrieved_jobs[f'{job_info_key.split(".", 1)[0]}'] = json.loads(redisConn.hget(redis_job_hash, job_info_key))

	return retrieved_jobs


def add_job_if_applicable(job, scheduler_p):
	job_id = str(job['id'])
	if not scheduler_p.get_job(job_id):
		"""add job to job list cache"""
		scheduled_jobs[job_id] = job
		"""add job to schedule"""
		add_job_to_scheduler(job, scheduler_p)

		print(f'added job with id: {str(job_id)} \nnext execution: {str(job["next_execution_time"])}')

	elif job_id not in scheduled_jobs:
		"""add job to job list cache"""
		scheduled_jobs[job_id] = job


def update_job_if_applicable(job, scheduler_p):
	job_id = str(job['id'])
	"""job can only be updated if it already exists"""
	if job_id not in scheduled_jobs:
		return

	"""check for changed version"""
	last_version = scheduled_jobs[job_id]['version']
	current_version = job['version']
	if current_version != last_version:
		"""refresh job in cache"""
		scheduled_jobs[job_id] = job
		"""add refreshed job to schedule"""
		remove_job(job_id, scheduler_p, True)
		add_job_to_scheduler(job, scheduler_p)
		print(f'updated job with id: {str(job_id)} \nnext execution: {str(job["next_execution_time"])}')


def add_job_to_scheduler(job, scheduler_p):
	job_l = Job(job)
	if job['cron_expression'] != '':
		new_job = scheduler_p.add_job(job_l.execute, CronTrigger.from_crontab(job['cron_expression']), id=str(job['id']), coalesce=True, misfire_grace_time=86400)
	elif job['execution_datetime'] != '':
		new_job = scheduler_p.add_job(job_l.execute, DateTrigger(dateConversion.initialize_datetime_from_string(job['execution_datetime']), timezone='UTC'), id=str(job['id']), coalesce=True, misfire_grace_time=86400)

	job['next_execution_time'] = new_job.next_run_time
	""" create Field "{job_id}.info" """
	redisConn.hset(redis_job_hash, f'{str(job["id"])}.info', json.dumps(job, default=str))


def reschedule_job(job_id, epoch_time):
	jobs_l = retrieve_jobs_from_redis()
	new_cron_expression = dateConversion.convert_epoch_to_cron_expression(epoch_time)
	if jobs_l[job_id]["cron_expression"] != new_cron_expression:
		adjust_job_property(job_id, "cron_expression", new_cron_expression)
		set_job_status(job_id, "requested")
		print(f'job {job_id} was rescheduled: ')


def remove_job(job_id, scheduler_p, silent):
	if job_id == "scheduler-job-id":
		return

	if not scheduler_p.get_job(job_id) and (job_id not in scheduled_jobs):
		return

	"""remove job from scheduler"""
	job_id = int(job_id)
	if scheduler_p.get_job(job_id):
		scheduler_p.remove_job(job_id)
	"""remove job from cached jobs"""
	if job_id in scheduled_jobs:
		scheduled_jobs.pop(job_id)

	"""print removed job incl. status"""
	if not scheduler_p.get_job(job_id) and (job_id not in scheduled_jobs):
		jobs_l = retrieve_jobs_from_redis()
		if not silent:
			print(f'removed job from schedule: id={job_id} status={jobs_l[job_id]["status"]}')


def restart_job(job_id):
	jobs_l = retrieve_jobs_from_redis()
	set_job_status(job_id, "requested")


def set_job_status(job_id, status):
	adjust_job_property(job_id, 'status', status)


def adjust_job_property(job_id, property_name, value):
	job_id = str(job_id)

	if job_id == "scheduler-job-id":
		return

	# job_index = 0
	"""retrieve most recent job list"""
	jobs_l = retrieve_jobs_from_redis()

	"""set new value"""
	jobs_l[job_id][property_name] = str(value)
	"""set new update timestamp"""
	jobs_l[job_id]["last_update"] = str(datetime.utcnow())
	"""increment version"""
	if property_name not in ["verson", "status", "last_execution_time"]:
		jobs_l[job_id]["version"] = int(jobs_l[job_id]["version"]) + 1

	"""save json"""
	#f = open("jobs.json", "w")
	#json.dump(jobs_l, f)
	#f.close

	redisConn.hset(redis_job_hash, f'{job_id}.info', json.dumps(jobs_l[job_id]))

	"""refresh job in cache"""
	if str(scheduled_jobs.get(job_id)) != str(jobs_l[job_id]):
		scheduled_jobs[job_id] = jobs_l[job_id]


def refresh_run_timestamp(job_id):
	adjust_job_property(job_id, "last_execution_time", datetime.utcnow())
