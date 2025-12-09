from apscheduler.schedulers.background import BackgroundScheduler
from app.services.sync_manager import full_sync

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(full_sync, "interval", minutes=30)
    scheduler.start()
