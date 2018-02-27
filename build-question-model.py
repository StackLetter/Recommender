#!env/bin/python

from recommender import train, psql

with psql:
    cur = psql.cursor()
    cur.execute("SELECT id, title, body FROM questions WHERE site_id = 3 AND removed is NULL AND created_at >= now() - interval '14 days';")

    i = 0
    for question in cur:
        topics = train.get_question_topics(question)
        train.persist_question_topics(question, topics)
        i+=1
        if i % 100 == 0:
            print(i)

