#!env/bin/python
import sys

if len(sys.argv) < 2:
    print("No day interval specified.")
    sys.exit(1)
try:
    days = int(sys.argv[1])
except ValueError:
    print("Invalid day interval specified; must be a number.")
    sys.exit(1)

from recommender import train, psql, config, utils

with psql:
    cur = psql.cursor()
    cur.execute(utils.queries.all_questions_since, (config.site_id, days))

    i = 0
    for question in cur:
        topics = train.get_question_topics(question)
        train.persist_question_topics(question, topics)
        i+=1
        if i % 100 == 0:
            print(i)

