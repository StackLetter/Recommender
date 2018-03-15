#!env/bin/python
from recommender import db, config, queries, utils

def run_weekly_cron(logger):
    logger.info('Retraining weekly subscriber user profiles')
    from recommender.profiles import UserProfile
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(queries.weekly_subscribers, (config.site_id,))
        i = 0
        for uid in cur:
            user = UserProfile.load(uid[0])
            user.retrain()
            user.save()
            utils.archive_user_profile(user)
            i+=1
    logger.debug('Retrained %d user profiles', i)
    db.close()


if __name__ == '__main__':
    logger = utils.setup_logging(config.cron_log_file_weekly)
    logger.info('Running weekly cron job')
    run_weekly_cron(logger)
    logger.info('Weekly cronjob finished')
