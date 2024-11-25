from apscheduler.schedulers.background import BackgroundScheduler
from app.neo4j_access.community_detection import CommunityDetection
from app.neo4j_access.update_functionality import UpdateFunctionality
from app.db import get_db
scheduler = BackgroundScheduler()

def start_scheduler():
    """
    Start the scheduler of automatically run the community detection and fetch new data from Opensea.  
    The frequency is set based on value stored in the neo4j db.
    """

    uf = UpdateFunctionality()
    if not scheduler.running:
        scheduler.start()
    cd =CommunityDetection()
    uf= UpdateFunctionality()
    ape_frequency = uf.get_update_frequency('boredapeyachtclub')
    degods_frequency = uf.get_update_frequency('degods-eth')
    add_job(uf.run_update, 'interval', seconds=ape_frequency, id='boredapeyachtclub_query_new_data', args=["boredapeyachtclub"])
    add_job(uf.run_update, 'interval', seconds=degods_frequency, id='degods-eth_query_new_data', args=["degods-eth"])
    
    add_job(cd.run_community_detection, 'interval', seconds=ape_frequency, id='boredapeyachtclub_run_com', args=[100,"boredapeyachtclub"])
    add_job(cd.run_community_detection, 'interval', seconds=degods_frequency, id='degods-eth_run_com', args=[100,"degods-eth"])
    add_job(cd.run_community_detection, 'interval', seconds=degods_frequency, id='complete_run_com', args=[100,"complete"])


def shutdown_scheduler():

    if scheduler.running:
        scheduler.shutdown()

def add_job(func, trigger, **kwargs):
    scheduler.add_job(func, trigger, **kwargs)

def remove_job(job_id):
    scheduler.remove_job(job_id)

def get_jobs():
    jobs = scheduler.get_jobs()
    return [ {"job_id": job.id,
                "next_run_time": job.next_run_time,
                "trigger": str(job.trigger)} for job in jobs]
    
def get_scheduler():
    return scheduler


