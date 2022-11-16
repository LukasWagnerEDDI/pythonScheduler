import redis
import json


redisConn = redis.StrictRedis(host='localhost', port=6379, db=0)
redis_job_hash = 'apscheduler.jobs'
job_info_tag = '{0}.info'
scheduled_jobs = {}


def get_jobs():
    job_info_keys = []
    retrieved_jobs = {}

    fields = redisConn.hgetall(redis_job_hash)
    keys = fields.keys()
    for key in keys:
        key = str(bytes(key).decode())
        #collect job.info keys
        if '.info' in key:
            job_info_keys.append(key)

    #retrieve all jobs' info
    for job_info_key in job_info_keys:
        job_id = f'{job_info_key.split(".", 1)[0]}'
        retrieved_jobs[job_id] = get_job_info(job_id)
    return retrieved_jobs


def get_scheduled_jobs():
    scheduled_keys = []
    scheduled_jobs = {}

    fields = redisConn.hgetall(redis_job_hash)
    keys = fields.keys()
    for key in keys:
        key = str(bytes(key).decode())
        #collect keys of scheduled jobs
        if not '.info' in key:
            scheduled_keys.append(key)

    #cache info of scheduled_jobs
    for scheduled_key in scheduled_keys:
        redis_entry = get_job_info(scheduled_key)

        #insert scheduled job
        if not scheduled_key in scheduled_jobs:
            scheduled_jobs[scheduled_key] = redis_entry

        #refresh scheduled job
        elif scheduled_jobs[scheduled_key] != redis_entry:
            scheduled_jobs[scheduled_key] = redis_entry
    
    return scheduled_jobs

def set_job_info(job):
    redisConn.hset(redis_job_hash, job_info_tag.format(str(job["id"])), json.dumps(job, default=str))


def get_job_info(job_id):
    return json.loads(str(bytes(redisConn.hget(redis_job_hash, job_info_tag.format(job_id))).decode()))

def get_job(job_id):
    return redisConn.hget(redis_job_hash, job_id)







