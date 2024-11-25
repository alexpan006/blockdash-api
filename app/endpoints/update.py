from fastapi import APIRouter
from typing import List
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from app.cache import connect_to_redis
from app.cache import delete_cache_keys
from app.apscheduler import add_job,get_jobs,shutdown_scheduler,get_scheduler,remove_job
from app.opensea_api.query_api import get_current_blocktime,convert_unix_to_blockNumber
# from app.neo4j_access.neo4j_access import db  # Assuming db is accessible here
from app.db import get_db
from app.neo4j_access.community_detection import CommunityDetection
from app.neo4j_access.update_functionality import UpdateFunctionality
from app.stats_com import testing_stats
update_router = APIRouter()

@update_router.get('/test_update')
def setUpUpdate(collection_name:str='boredapeyachtclub'): #5days
    """
    Method for testing purpose to run the update progress.
    
    """
    uf= UpdateFunctionality()
    try:
        uf.run_update(collection_name)
    except Exception as err:
        print(err)
    return "haha"

@update_router.get("/shutdown")
def shutdown_event():
    shutdown_scheduler()


@update_router.get("/test")
def testing():
    testing_stats()


@update_router.get("/remove_job")
def removeJob(job_id:str="None"):
    if job_id == "None":
        return "Please specify the job id you want to remove"
    else:
        remove_job(job_id)
        return "Remove complete"
    
@update_router.get("/get_frequency")
def get_frequency(collection_name:str = "boredapeyachtclub"):
    uf= UpdateFunctionality()
    result = uf.get_update_frequency(collection_name)
    return result

@update_router.get("/set_frequency")
def get_frequency(collection_name:str = "boredapeyachtclub",frequency:int = 7200):
    uf= UpdateFunctionality()
    uf.set_update_frequency(collection_name,frequency)
    return "set!"
   
@update_router.get("/get_all_jobs")
def getJobs():
    return get_jobs()

@update_router.get("/get_blockNumber")
def get_blockNumber():
    return get_current_blocktime()

@update_router.get("/convert_unix")
def convert_unix(unix:int=1619817106):
    return convert_unix_to_blockNumber(unix)

@update_router.delete("/cache")
def getJobs():
    return delete_cache_keys("application-cache:*")


@update_router.delete("/trigger_instanly")
def trigger():
    sch= get_scheduler()
    sch.modify_job(next_run_time=datetime.now())



@update_router.get('/times')
def getlastUpdate():

    redis_connection = connect_to_redis()
    lastUpdateRedis = redis_connection.get("lastUpdateAt")
    
    jobTimes = []
    jobs = get_jobs()

    if jobs:
        for job in jobs:
            jobTimes.append(job["next_run_time"])
        
        jobTimes.sort()
        nextUpdateObj = jobTimes.pop()
        nextUpdate = nextUpdateObj.strftime("%d.%m.%Y %H:%M")
    else:
        nextUpdate = "not available"

    if lastUpdateRedis:
        lastUpdate = datetime.fromtimestamp(float(lastUpdateRedis)).strftime("%d.%m.%Y %H:%M")
    else:
        lastUpdate = "not available"

    return {
        "lastUpdateAt": lastUpdate,
        "nextUpdateAt": nextUpdate
    }