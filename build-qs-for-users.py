#!env/bin/python
import sys

from recommender import train, psql, config

with psql:
    cur = psql.cursor()
    cur.execute("""
WITH question_ids AS (
    SELECT id FROM (
        WITH
            site_users AS (SELECT id FROM users WHERE account_id is not null AND site_id = %s),
            commented_answers AS (SELECT answer_id AS id FROM comments WHERE question_id is null AND owner_id IN (SELECT id FROM site_users))
        -- Questions
        SELECT id FROM questions WHERE owner_id IN (SELECT id FROM site_users)
        UNION
        -- Answers
        SELECT question_id AS id FROM answers WHERE owner_id IN (SELECT id FROM site_users)
        UNION
        -- Comments on questions
        SELECT question_id AS id FROM comments WHERE question_id is not null AND owner_id IN (SELECT id FROM site_users)
        UNION
        -- Comments on answers
        SELECT question_id AS id FROM answers WHERE id IN (SELECT id FROM commented_answers)
    ) united
)
SELECT id, title, body FROM questions WHERE id IN (SELECT id FROM question_ids);
""", (config.site_id,))

    for question in cur:
        topics = train.get_question_topics(question)
        train.persist_question_topics(question, topics)

