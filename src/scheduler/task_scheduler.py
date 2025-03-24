import os
import sys
import logging
import time
import schedule
import threading
from typing import Callable, Optional, List

# 修复导入路径问题
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import settings

logger = logging.getLogger(__name__)

class TaskScheduler:
    """Schedules and manages recurring tasks."""
    
    def __init__(self, interval_seconds: int = None):
        """Initialize the task scheduler.
        
        Args:
            interval_seconds: Interval between task executions in seconds
        """
        self.interval_seconds = interval_seconds or settings.SCAN_INTERVAL_SECONDS
        self.running = False
        self.scheduler_thread = None
        self.scheduled_jobs = []
    
    def _run_scheduler(self):
        """Run the scheduler in a separate thread."""
        self.running = True
        logger.info(f"Starting scheduler with {self.interval_seconds}s interval")
        
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def start(self):
        """Start the scheduler in a separate thread."""
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            logger.warning("Scheduler is already running")
            return
            
        self.scheduler_thread = threading.Thread(target=self._run_scheduler)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        logger.info("Scheduler started in background thread")
    
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5.0)
            logger.info("Scheduler stopped")
        
        # Clear all scheduled jobs
        schedule.clear()
        self.scheduled_jobs = []
    
    def add_job(self, task_func: Callable, job_id: Optional[str] = None) -> str:
        """Add a new job to the scheduler.
        
        Args:
            task_func: Function to execute on schedule
            job_id: Optional identifier for the job
            
        Returns:
            Job identifier
        """
        if not job_id:
            job_id = f"job_{len(self.scheduled_jobs) + 1}"
            
        job = schedule.every(self.interval_seconds).seconds.do(task_func)
        self.scheduled_jobs.append((job_id, job))
        
        logger.info(f"Added job '{job_id}' to run every {self.interval_seconds} seconds")
        return job_id
    
    def remove_job(self, job_id: str) -> bool:
        """Remove a job from the scheduler.
        
        Args:
            job_id: Identifier of the job to remove
            
        Returns:
            True if job was removed, False otherwise
        """
        for idx, (jid, job) in enumerate(self.scheduled_jobs):
            if jid == job_id:
                schedule.cancel_job(job)
                self.scheduled_jobs.pop(idx)
                logger.info(f"Removed job '{job_id}'")
                return True
        
        logger.warning(f"Job '{job_id}' not found")
        return False
    
    def list_jobs(self) -> List[str]:
        """List all scheduled jobs.
        
        Returns:
            List of job identifiers
        """
        return [job_id for job_id, _ in self.scheduled_jobs]
