#!env/bin/python
from recommender import train, db, config, queries, utils

def create_question_profiles(query, args):
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(query, args)

        i = 0
        for question in cur:
            topics = train.get_question_topics(question)
            train.persist_question_topics(question, topics)
            i+=1
        return i

def run_daily_cron(logger):
    logger.info('Creating question profiles from last 2 days')
    cnt = create_question_profiles(queries.all_questions_since, (config.site_id, 2))
    logger.debug('Created %d profiles', cnt)

    logger.info('Creating profiles for all user activities')
    cnt = create_question_profiles(queries.all_user_activity, (config.site_id,))
    logger.debug('Created %d profiles', cnt)

    logger.info('Retraining daily subscriber user profiles')
    from recommender.profiles import UserProfile, CommunityProfile
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(queries.daily_subscribers, (config.site_id,))
        i = 0
        for uid in cur:
            user = UserProfile.load(uid[0])
            user.retrain()
            user.save()
            utils.archive_user_profile(user)
            i+=1
    logger.debug('Retrained %d user profiles', i)

    # 4) Retrain community user profile
    logger.info('Retraining community profile')
    community = CommunityProfile.load()
    community.retrain()
    community.save()
    utils.archive_user_profile(community)
    logger.info('Community profile saved')

    db.close()


if __name__ == '__main__':
    logger = utils.setup_logging(config.cron_log_file_daily)
    logger.info('Running daily cron job')
    run_daily_cron(logger)
    logger.info('Daily cronjob finished')
