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
        UNION
        -- Favorited questions
        SELECT q.id AS id FROM user_favorites f
        LEFT JOIN questions q ON q.external_id = f.external_id
        WHERE q.id IS NOT NULL
        UNION
        -- Questions with feedback
        SELECT e.content_detail::int AS id FROM evaluation_newsletters e
        LEFT JOIN newsletters n ON n.id = e.newsletter_id
        WHERE e.content_type = 'question' AND e.user_response_type IN ('click', 'feedback')
        AND n.user_id IN (SELECT id FROM site_users)
        UNION
        -- Answers with feedback
        SELECT a.question_id AS id FROM evaluation_newsletters e
        LEFT JOIN newsletters n ON n.id = e.newsletter_id
        LEFT JOIN answers a ON a.id = e.content_detail::int
        WHERE e.content_type = 'answer' AND e.user_response_type IN ('click', 'feedback')
        AND n.user_id IN (SELECT id FROM site_users)
    ) united
)
SELECT id, title, body FROM questions
WHERE id IN (SELECT id FROM question_ids)
AND id NOT IN (SELECT DISTINCT question_id FROM mls_question_topics)
AND removed IS NULL;
""", (config.site_id,))

    for question in cur:
        topics = train.get_question_topics(question)
        train.persist_question_topics(question, topics)

