#!env/bin/python

from recommender import train, db, config, queries

with db.connection() as conn:
    cur = conn.cursor()
    cur.execute(queries.all_user_activity, (config.site_id,))

    for question in cur:
        topics = train.get_question_topics(question)
        train.persist_question_topics(question, topics)


db.close()
