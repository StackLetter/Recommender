#!env/bin/python
from pathlib import Path
from recommender import train, psql, config, utils
from datetime import datetime

def archive_user_profile(user):
    archive_dir = Path('.') / config.archive_dir / '{:%Y-%m-%d}'.format(datetime.now())
    if not archive_dir.exists():
        archive_dir.mkdir(parents=True)
    return user.save(archive_dir / 'w_{}.pkl'.format(user.id))


# Retrain user profiles for all weekly newsletter subscribers
from recommender.profiles import UserProfile
with psql:
    cur = psql.cursor()
    cur.execute(utils.queries.weekly_subscribers, (config.site_id,))
    for uid in cur:
        user = UserProfile.load(uid[0])
        user.retrain()
        user.save()
        archive_user_profile(user)
