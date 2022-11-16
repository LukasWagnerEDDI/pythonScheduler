from os import error
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from datetime import datetime, timedelta
from subprocess import *
import dateConversion
import redisMgmt
import io


scheduled_jobs = redisMgmt.get_scheduled_jobs() #contains all in redis scheduled jobs

main_job_id = 'scheduler-job-id'
retry_reschedule_time = 60 #minutes
misfire_gracetime = 86400
last_scheduling = datetime.utcnow()

mainJobMissing = 'apscheduler main job "{main_job_id}" is missing\n'
addedJob = 'added job with id: {0} \nnext execution: {1}\n'
updatedJob = 'updated job with id: {0} \nnext execution: {1}\n'
rescheduledJob = 'job {0} was rescheduled: {1} -> {2}'
executingJob = 'executing job with id: {0}\n'
removedJob = 'removed job from schedule: id={0} status={1}'
scriptError = '\nscript execution of job {0} resulted in following error: \n\n returncode: {1} \n\nerror message: {2}\n\n'
errorAfterRetries = 'After 3 retries Job {0} still ran into an error\n'


class Job:
    def __init__(self, job=""):
        self.job = job

    def execute(self):
        execute_job(self.job)


def schedule_jobs(scheduler_p):
    #retrieve jobs from redis
    jobs = redisMgmt.get_jobs()
    
    if not scheduler_p.get_job(main_job_id):
        raise RuntimeError(mainJobMissing)

    for key in jobs.keys():
        if key in [main_job_id, redisMgmt.redis_job_hash]:
            continue

        job = jobs[key]

        #skip finished job
        if job["status"] in ["finished", "error", "missed"]:
            remove_job(job['id'], scheduler_p, False)
            continue

        if job['status'] == 'retry':
            if dateConversion.initialize_datetime_from_string(job['last_update']) > dateConversion.initialize_datetime_from_string(jobs[main_job_id]['last_execution_time']):
                remove_job(job['id'], scheduler_p, True)
                add_job_if_applicable(job, scheduler_p)
                continue

        #add new job to schedule
        add_job_if_applicable(job, scheduler_p)
        #update edited job - checking for different job version
        update_job_if_applicable(job, scheduler_p)

    refresh_run_timestamp(main_job_id)
    print("refreshed scheduled jobs")


def execute_job(job):
    job = redisMgmt.get_job_info(job["id"])
    print(executingJob.format(str(job["id"])))
    print(datetime.utcnow())
    #set readable run timestamp
    refresh_run_timestamp(job["id"])
    set_job_status(job["id"], "running")

    if '' != job["run_script"]:
        #create list of given parameters
        params = ["python", job["run_script"]] # "python" varies by environment: python or python3 needed
        params.extend(
            [f'{key}={val}' for key, val in job['script_parameters'].items() if val != ""]
        )

        try:
            #run script
            p = Popen(params, shell=True, stdout=PIPE, stderr=PIPE)
            p.wait()
            #evaluate subprocess' output
            if p.returncode != 0: #returncode 0 = processed without exception
                #save output of subprocess & raise error
                job['error_message'] = ''
                for line in io.TextIOWrapper(p.stderr, encoding="utf-8"):  # or another encoding
                    job['error_message'] += line + '\n'
                raise RuntimeError()
        except RuntimeError:
            #inform about error
            print(scriptError.format(job["id"], p.returncode, job["error_message"]))

            adjust_job_property(job['id'], 'error_message', job['error_message'])

            #reschedule or define as faulty(status=error)
            job = redisMgmt.get_job_info(job['id'])
            if int(job['retries']) < 3:
                #create next run datetime
                dt = datetime.fromtimestamp(dateConversion.convert_datetime_to_epoch(datetime.now() + timedelta(minutes=retry_reschedule_time))) # next execution in 1h
                reschedule_job(job['id'], 'retry', dt.timestamp())  
                print("\n\n")
            else:
                #throw error after 3 retries / 4th run of job
                raise RuntimeError(errorAfterRetries.format(job["id"]))

    job = redisMgmt.get_job_info(job['id'])
    if job['status'] == 'running':
        #set finished or success status
        if job["cron_expression"] != "":
            set_job_status(job["id"], "success")
        else:
            set_job_status(job["id"], "finished")


def add_job_if_applicable(job, scheduler_p):
    job_id = str(job['id'])
    if not scheduler_p.get_job(job_id) and not job_id in scheduled_jobs:
        #add job to job list cache
        scheduled_jobs[job_id] = job
        #add job to schedule
        add_job_to_scheduler(job, scheduler_p)

        print(addedJob.format(str(job_id), str(job["next_execution_time"])))


def update_job_if_applicable(job, scheduler_p):
    job_id = str(job['id'])
    #job can only be updated if it already exists
    if job_id not in scheduled_jobs:
        return

    #check for changed version
    last_version = scheduled_jobs[job_id]['version']
    current_version = job['version']
    if current_version != last_version:
        #add refreshed job to schedule
        remove_job(job_id, scheduler_p, True)
        add_job_to_scheduler(job, scheduler_p)
        print(updatedJob.format(str(job_id), str(job["next_execution_time"])))


def add_job_to_scheduler(job, scheduler_p):
    job_l = Job(job)
    if job['cron_expression'] != '':
        new_job = scheduler_p.add_job(job_l.execute, CronTrigger.from_crontab(job['cron_expression']),
                                      id=str(job['id']), coalesce=True, misfire_grace_time=misfire_gracetime)

    elif job['execution_datetime'] != '':
        new_job = scheduler_p.add_job(job_l.execute, DateTrigger(
            dateConversion.initialize_datetime_from_string(job['execution_datetime']), timezone='UTC'),
                                      id=str(job['id']), coalesce=True, misfire_grace_time=misfire_gracetime)

    job['next_execution_time'] = new_job.next_run_time
    #create Field "{job_id}.info"
    redisMgmt.set_job_info(job)
    
    #refresh scheduled job cache
    if not job['id'] in scheduled_jobs:
        scheduled_jobs[job['id']] = job


def reschedule_job(job_id, new_status, epoch_time):
    job_id = str(job_id)
    job_l = redisMgmt.get_job_info(job_id)
    new_execution_datetime = dateConversion.convert_epoch_to_datetime(epoch_time)

    #code for tasks scheduled by cron expression
    if job_l["cron_expression"] != "":
        old_execution_datetime = job_l["next_execution_time"]
        new_cron_expression = dateConversion.convert_epoch_to_cron_expression(epoch_time)
        if job_l["cron_expression"] != new_cron_expression:
            adjust_job_property(job_id, "cron_expression", new_cron_expression)

    #code for tasks scheduled by date time
    elif job_l['execution_datetime'] != '':
        old_execution_datetime = job_l["execution_datetime"]
        if job_l['execution_datetime'] != new_execution_datetime:
            adjust_job_property(job_id, "execution_datetime", new_execution_datetime)

    set_job_status(job_id, new_status)

    print(rescheduledJob.format(job_id, old_execution_datetime, new_execution_datetime))


def remove_job(job_id, scheduler_p, silent):
    if job_id == "scheduler-job-id":
        return

    if not scheduler_p.get_job(job_id) and (job_id not in scheduled_jobs):
        return

    #remove job from scheduler
    job_id = int(job_id)
    if scheduler_p.get_job(job_id):
        scheduler_p.remove_job(job_id)

    #remove job from cached jobs
    if str(job_id) in scheduled_jobs:
        scheduled_jobs.pop(str(job_id))

    #print removed job incl. status
    if not scheduler_p.get_job(job_id) and (job_id not in scheduled_jobs):
        job_l = redisMgmt.get_job_info(job_id)
        if not silent:
            print(removedJob.format(job_id, job_l["status"]))


def restart_job(job_id):
    set_job_status(job_id, "requested")


def set_job_status(job_id, status):
    adjust_job_property(job_id, 'status', status)


def adjust_job_property(job_id, property_name, value):
    job_id = str(job_id)

    if (job_id == "scheduler-job-id") and (property_name not in ["last_execution_time", "next_execution_time"]):
        return

    #retrieve most recent job info
    job_l = redisMgmt.get_job_info(job_id)

    #set new value
    job_l[property_name] = str(value)
    #set new update timestamp
    job_l["last_update"] = str(datetime.utcnow())
    #increment version
    if property_name not in ["version", "status", "last_execution_time", "next_execution_time"]:
        job_l["version"] = int(job_l["version"]) + 1

    #increment retry counter
    if (property_name == 'status') and (value == 'retry'):
        job_l['retries'] = int(job_l['retries']) + 1

    redisMgmt.set_job_info(job_l)

    #refresh job in cache
    if str(scheduled_jobs.get(job_id)) != str(job_l):
        scheduled_jobs[job_id] = job_l


def refresh_run_timestamp(job_id):
    adjust_job_property(job_id, "last_execution_time", datetime.utcnow())