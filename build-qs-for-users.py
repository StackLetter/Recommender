#!env/bin/python

from recommender import train, db, config, utils

with db.connection() as conn:
    cur = conn.cursor()
    cur.execute(utils.queries.all_user_activity, (config.site_id,))

    for question in cur:
        topics = train.get_question_topics(question)
        train.persist_question_topics(question, topics)


db.close()
