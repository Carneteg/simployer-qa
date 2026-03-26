"""
arq background worker.
Start with: python worker.py
"""
import logging
import arq

from config import settings
from services.evaluator import evaluate_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


class WorkerSettings:
    functions      = [evaluate_run]
    redis_settings = settings.redis_settings
    max_jobs       = 4
    job_timeout    = 7200
    keep_result    = 3600


if __name__ == "__main__":
    arq.run_worker(WorkerSettings)
