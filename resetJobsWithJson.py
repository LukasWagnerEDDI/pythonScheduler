from jobMgmt import *

if __name__ == '__main__':
	jobs = retrieve_jobs_to_schedule()

	for job in jobs:
		job['version'] = job['version'] + 1
		adjust_job_property_json(job['id'], 'status', job['version'] + 1)

		redisConn.hset('apscheduler.jobs', f'{job["id"]}.info', json.dumps(job))

