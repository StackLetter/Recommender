#!env/bin/python
from pathlib import Path
from recommender import train, db, config, utils
from datetime import datetime

def create_question_profiles(query, args):
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute(query, args)

        for question in cur:
            topics = train.get_question_topics(question)
            train.persist_question_topics(question, topics)

def archive_user_profile(user):
    archive_dir = Path('.') / config.archive_dir / '{:%Y-%m-%d}'.format(datetime.now())
    if not archive_dir.exists():
        archive_dir.mkdir(parents=True)
    return user.save(archive_dir / '{}.pkl'.format(user.id))



# 1) Create profiles for Qs from last two days
create_question_profiles(utils.queries.all_questions_since, (config.site_id, 2))

# 2) Create profiles for Qs in all user activities
create_question_profiles(utils.queries.all_user_activity, (config.site_id,))

# 3) Retrain user profiles for all daily newsletter subscribers
from recommender.profiles import UserProfile
with db.connection() as conn:
    cur = conn.cursor()
    cur.execute(utils.queries.daily_subscribers, (config.site_id,))
    for uid in cur:
        user = UserProfile.load(uid[0])
        user.retrain()
        user.save()
        archive_user_profile(user)

# 4) Retrain community user profile TODO


db.close()
