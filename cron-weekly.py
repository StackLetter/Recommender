#!env/bin/python
from pathlib import Path
from recommender import db, config, queries
from datetime import datetime

def archive_user_profile(user):
    archive_dir = Path('.') / config.archive_dir / '{:%Y-%m-%d}'.format(datetime.now())
    if not archive_dir.exists():
        archive_dir.mkdir(parents=True)
    return user.save(archive_dir / 'w_{}.pkl'.format(user.id))

def run_weekly_cron():
    # Retrain user profiles for all weekly newsletter subscribers
    from recommender.profiles import UserProfile
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(queries.weekly_subscribers, (config.site_id,))
        for uid in cur:
            user = UserProfile.load(uid[0])
            user.retrain()
            user.save()
            archive_user_profile(user)
    db.close()


if __name__ == '__main__':
    run_weekly_cron()
