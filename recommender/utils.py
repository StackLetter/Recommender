import random
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
from recommender import config
from datetime import datetime

def setup_logging(log_file, logger=None):
    log_format = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'

    log_file = Path('.') / log_file
    if not log_file.parent.exists():
        log_file.parent.mkdir(parents=True)
    log_level = logging.DEBUG  # logging.DEBUG if debug_mode else logging.INFO
    log_handler = RotatingFileHandler(str(log_file), maxBytes=1e7, backupCount=10)
    log_handler.setLevel(log_level)
    log_handler.setFormatter(logging.Formatter(log_format))
    if logger is None:
        logging.basicConfig(format=log_format)
        logger = logging.root
    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger


def archive_user_profile(user, when=None):
    if not when:
        when = datetime.now()
    archive_dir = Path('.') / config.archive_dir / '{:%Y-%m-%d}'.format(when)
    if not archive_dir.exists():
        archive_dir.mkdir(parents=True)
    return user.save(archive_dir / '{}.pkl'.format(user.id))


def weighted_choice(choices):
   total = sum(w for c, w in choices)
   r = random.uniform(0, total)
   upto = 0
   for c, w in choices:
      if upto + w >= r:
         return c, w
      upto += w
   assert False, "Shouldn't get here"

