#!env/bin/python

from recommender import train, psql, config, utils

with psql:
    cur = psql.cursor()
    cur.execute(utils.queries.all_user_activity, (config.site_id,))

    for question in cur:
        topics = train.get_question_topics(question)
        train.persist_question_topics(question, topics)

