from types import SimpleNamespace
import flask

queries = SimpleNamespace()

queries.all_user_activity = """
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
"""

queries.all_questions_since = """
SELECT id, title, body FROM questions
WHERE site_id = %s AND removed IS NULL
AND created_at >= now() - interval '%s days'
AND id NOT IN (SELECT DISTINCT question_id FROM mls_question_topics)"""

queries.daily_subscribers = """
SELECT u.id FROM users u LEFT JOIN accounts a ON u.account_id = a.id
WHERE account_id IS NOT NULL AND site_id = %s AND a.frequency = 'd'"""

queries.weekly_subscribers = """
SELECT u.id FROM users u LEFT JOIN accounts a ON u.account_id = a.id
WHERE account_id IS NOT NULL AND site_id = %s AND a.frequency = 'w'"""

queries.question_answer_index = 'SELECT question_id, id FROM answers WHERE id IN %(answers)s'


queries.sections = {
    'hot-questions': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            LEFT JOIN question_tags qt ON qt.question_id = q.id
            LEFT JOIN mls_question_topics qto ON qto.question_id = q.id
            WHERE q.site_id = %(site_id)s
            AND q.score > 3
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",

    'useful-questions': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            LEFT JOIN answers a ON q.id = a.question_id
            LEFT JOIN question_tags qt ON qt.question_id = q.id
            LEFT JOIN mls_question_topics qto ON qto.question_id = q.id
            WHERE q.site_id = %(site_id)s
            AND q.score > 3
            AND a.question_id IS NOT NULL
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",

    'awaiting-answer': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            LEFT JOIN answers a ON q.id = a.question_id
            LEFT JOIN question_tags qt ON qt.question_id = q.id
            LEFT JOIN mls_question_topics qto ON qto.question_id = q.id
            WHERE q.site_id = %(site_id)s
            AND a.question_id IS NULL
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)
            ) x
        ORDER BY score ASC, creation_date DESC
        LIMIT 500""",

    'popular-unanswered': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            LEFT JOIN answers a ON q.id = a.question_id
            LEFT JOIN question_tags qt ON qt.question_id = q.id
            LEFT JOIN mls_question_topics qto ON qto.question_id = q.id
            WHERE q.site_id = %(site_id)s
            AND q.score > 1
            AND a.question_id IS NULL
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",

    'highly-discussed-qs': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.comment_count, q.creation_date
            FROM questions q
            LEFT JOIN question_tags qt ON qt.question_id = q.id
            LEFT JOIN mls_question_topics qto ON qto.question_id = q.id
            WHERE q.site_id = %(site_id)s
            AND q.id NOT IN %(dupes)s
            AND q.comment_count > 3
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)
            ) x
        ORDER BY comment_count DESC, creation_date DESC
        LIMIT 500""",

    'highly-discussed-as': """
        SELECT DISTINCT * FROM (
            SELECT a.id, a.comment_count, a.creation_date
            FROM answers a
            LEFT JOIN questions q ON q.id = a.question_id
            LEFT JOIN question_tags qt ON qt.question_id = q.id
            LEFT JOIN mls_question_topics qto ON qto.question_id = q.id
            WHERE a.site_id = %(site_id)s
            AND a.id NOT IN %(dupes)s
            AND a.comment_count > 3
            AND a.removed IS NULL
            AND a.creation_date > %(since)s
            AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)
            ) x
        ORDER BY comment_count DESC, creation_date DESC
        LIMIT 500""",

    'interesting-answers': """
        SELECT DISTINCT * FROM (
            SELECT a.id, a.score, a.creation_date
            FROM answers a
            LEFT JOIN questions q ON q.id = a.question_id
            LEFT JOIN question_tags qt ON qt.question_id = q.id
            LEFT JOIN mls_question_topics qto ON qto.question_id = q.id
            WHERE a.site_id = %(site_id)s
            AND a.id NOT IN %(dupes)s
            AND a.score > 1
            AND a.removed IS NULL
            AND a.creation_date > %(since)s
            AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",
}
