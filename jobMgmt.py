from os import error
import sys
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, timedelta
from subprocess import *
import json
import dateConversion
import redis
import io

main_job_id = 'scheduler-job-id'
redisConn = redis.StrictRedis(host='localhost', port=6379, db=0)
last_scheduling = datetime.now()
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
    scheduler_job = scheduler_p.get_job(redis_job_hash)

    for key in jobs.keys():
        if key in [main_job_id, redis_job_hash]:
            continue

        job = jobs[key]

        """skip finished job"""
        if job["status"] in ["finished", "error", "missed"]:
            remove_job(job['id'], scheduler_p, False)
            continue

        if job['status'] == 'retry':
            if dateConversion.initialize_datetime_from_string(job['last_update']) > dateConversion.initialize_datetime_from_string(jobs[main_job_id]['last_execution_time']):
                remove_job(job['id'], scheduler_p, False)
                add_job_if_applicable(job, scheduler_p)
                continue

        """add new job to schedule"""
        add_job_if_applicable(job, scheduler_p)
        """update edited job - checking for different job version"""
        update_job_if_applicable(job, scheduler_p)

    refresh_run_timestamp(main_job_id)
    print("refreshed scheduled jobs")


def execute_job(job):
    jobs = retrieve_jobs_from_redis()
    job = jobs[str(job['id'])]

    print("executing job with id: " + str(job['id']))
    print(datetime.utcnow())
    """set readable run timestamp"""
    refresh_run_timestamp(job["id"])
    set_job_status(job["id"], "running")

    if '' != job["run_script"]:
        """create list of given parameters"""
        params = ["python", job["run_script"]] # "python" varies by environment: python or python3 needed
        params.extend(
            [f'{key}={val}' for key, val in job['script_parameters'].items() if val != ""]
        )

        try:
            """run script"""
            p = Popen(params, shell=True, stdout=PIPE, stderr=STDOUT)
            """evaluate subprocess' output"""
            if p.returncode != 0: #returncode 0 = processed without exception
                """save output of subprocess & raise error"""
                job['error_message'] = ''
                for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):  # or another encoding
                    job['error_message'] += line + '\n'
                raise RuntimeError()
        except RuntimeError:
            """inform about error"""
            print(
                f'\nscript execution of job {job["id"]} resulted in following error: \n\n returncode: {p.returncode} \n\nerror message: {job["error_message"]}\n\n')
            adjust_job_property(job['id'], 'error_message', job['error_message'])

            """reschedule or define as faulty(status=error)"""
            retrieve_job_from_redis(job['id'])
            if int(job['retries']) < 3:
                """create next run datetime"""
                dt = datetime.fromtimestamp(dateConversion.convert_datetime_to_epoch(datetime.now() + timedelta(hours=1))) # next execution in 1h
                reschedule_job(job['id'], 'retry', dt.timestamp())  
                print("\n\n")
            else:
                """throw error after 3 retries / 4th run of job"""
                raise RuntimeError(f'After 3 retries Job {job["id"]} still ran into an error')

    if job['status'] == 'running':
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

def retrieve_job_from_redis(job_id):
    return bytes(redisConn.hget(redis_job_hash, f'{job_id}.info')).decode()

def add_job_if_applicable(job, scheduler_p):
    job_id = str(job['id'])
    if not scheduler_p.get_job(job_id) and not job_id in scheduled_jobs:
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
        new_job = scheduler_p.add_job(job_l.execute, CronTrigger.from_crontab(job['cron_expression']),
                                      id=str(job['id']), coalesce=True, misfire_grace_time=86400)
    elif job['execution_datetime'] != '':
        new_job = scheduler_p.add_job(job_l.execute, DateTrigger(
            dateConversion.initialize_datetime_from_string(job['execution_datetime']), timezone='UTC'),
                                      id=str(job['id']), coalesce=True, misfire_grace_time=86400)

    job['next_execution_time'] = new_job.next_run_time
    """ create Field "{job_id}.info" """
    redisConn.hset(redis_job_hash, f'{str(job["id"])}.info', json.dumps(job, default=str))


def reschedule_job(job_id, new_status, epoch_time):
    job_id = str(job_id)
    jobs_l = retrieve_jobs_from_redis()
    new_execution_datetime = dateConversion.convert_epoch_to_datetime(epoch_time)

    """code for tasks scheduled by cron expression"""
    if jobs_l[job_id]["cron_expression"] != "":
        old_execution_datetime = jobs_l[job_id]["next_execution_time"]
        new_cron_expression = dateConversion.convert_epoch_to_cron_expression(epoch_time)
        if jobs_l[job_id]["cron_expression"] != new_cron_expression:
            adjust_job_property(job_id, "cron_expression", new_cron_expression)
    # """code for tasks scheduled by date time"""
    elif jobs_l[job_id]['execution_datetime'] != '':
        old_execution_datetime = jobs_l[job_id]["execution_time"]
        if jobs_l[job_id]['execution_datetime'] != new_execution_datetime:
            adjust_job_property(job_id, "execution_datetime", new_execution_datetime)

    set_job_status(job_id, new_status)

    print(f'job {job_id} was rescheduled: {old_execution_datetime} -> {new_execution_datetime}')


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
    if str(job_id) in scheduled_jobs:
        scheduled_jobs.pop(str(job_id))

    """print removed job incl. status"""
    if not scheduler_p.get_job(job_id) and (str(job_id) not in scheduled_jobs):
        jobs_l = retrieve_jobs_from_redis()
        if not silent:
            print(f'removed job from schedule: id={job_id} status={jobs_l[str(job_id)]["status"]}')


def restart_job(job_id):
    jobs_l = retrieve_jobs_from_redis()
    set_job_status(job_id, "requested")


def set_job_status(job_id, status):
    adjust_job_property(job_id, 'status', status)


def adjust_job_property(job_id, property_name, value):
    job_id = str(job_id)

    if (job_id == "scheduler-job-id") and (property_name not in ["last_execution_time", "next_execution_time"]):
        return

    """retrieve most recent job list"""
    jobs_l = retrieve_jobs_from_redis()

    """set new value"""
    jobs_l[job_id][property_name] = str(value)
    """set new update timestamp"""
    jobs_l[job_id]["last_update"] = str(datetime.utcnow())
    """increment version"""
    if property_name not in ["version", "status", "last_execution_time", "next_execution_time"]:
        jobs_l[job_id]["version"] = int(jobs_l[job_id]["version"]) + 1

    """increment retry counter"""
    if (property_name == 'status') and (value == 'retry'):
        jobs_l[job_id]['retries'] = int(jobs_l[job_id]['retries']) + 1

    redisConn.hset(redis_job_hash, f'{job_id}.info', json.dumps(jobs_l[job_id]))

    """refresh job in cache"""
    if str(scheduled_jobs.get(job_id)) != str(jobs_l[job_id]):
        scheduled_jobs[job_id] = jobs_l[job_id]


def refresh_run_timestamp(job_id):
    adjust_job_property(job_id, "last_execution_time", datetime.utcnow())