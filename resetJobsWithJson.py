import os
from jobMgmt import *

last_job_file_change = os.stat('jobs.json')
last_job_file_change = datetime.fromtimestamp(last_job_file_change.st_mtime)
retrieved_jobs_json = {
	"jobs": (),
	"last_job_file_change": f'{last_job_file_change}'
}


def get_index_for_job_id(job_list, job_id):
	for index, job in enumerate(job_list):
		if job['id'] == job_id:
			return index


def adjust_job_property_json(job_id, property_name, value):
	if job_id == "scheduler-job-id":
		return

	job_index = 0
	"""retrieve most recent job list"""
	jobs_l = retrieve_jobs_to_schedule()
	job_index = get_index_for_job_id(jobs_l, job_id)

	"""set new value"""
	jobs_l[job_index][property_name] = str(value)
	"""set new update timestamp"""
	jobs_l[job_index]["last_update"] = str(datetime.utcnow())
	"""increment version"""
	if property_name not in ["version", "status", "last_execution_time"]:
		jobs_l[job_index]["version"] = int(jobs_l[job_index]["version"]) + 1

	"""save json"""
	f = open("jobs.json", "w")
	json.dump(jobs_l, f)
	f.close()

	"""refresh job in cache"""
	if str(scheduled_jobs.get(job_id)) != str(jobs_l[job_index]):
		scheduled_jobs[job_id] = jobs_l[job_index]


def retrieve_jobs_to_schedule():
	"""check if file was changed"""
	current_file_stat = os.stat('jobs.json')
	current_file_change = datetime.fromtimestamp(current_file_stat.st_mtime)

	if dateConversion.initialize_datetime_from_string(retrieved_jobs_json['last_job_file_change']) < current_file_change:
		with open('jobs.json') as f:
			retrieved_jobs_json['jobs'] = json.load(f)
		retrieved_jobs_json['last_job_file_change'] = f'{current_file_change}'

	return retrieved_jobs_json['jobs']


if __name__ == '__main__':
	jobs = retrieve_jobs_to_schedule()

	for job in jobs:
		job['version'] = job['version'] + 1
		adjust_job_property_json(job['id'], 'status', job['version'] + 1)

		redisConn.hset('apscheduler.jobs', f'{job["id"]}.info', json.dumps(job))

