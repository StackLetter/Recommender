
all_user_activity = """
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

all_questions_since = """
SELECT id, title, body FROM questions
WHERE site_id = %s AND removed IS NULL
AND created_at >= now() - interval '%s days'
AND id NOT IN (SELECT DISTINCT question_id FROM mls_question_topics)"""

daily_subscribers = """
SELECT u.id FROM users u LEFT JOIN accounts a ON u.account_id = a.id
WHERE account_id IS NOT NULL AND site_id = %s AND a.frequency = 'd'"""

weekly_subscribers = """
SELECT u.id FROM users u LEFT JOIN accounts a ON u.account_id = a.id
WHERE account_id IS NOT NULL AND site_id = %s AND a.frequency = 'w'"""

question_answer_index = 'SELECT question_id, id FROM answers WHERE id IN %(answers)s'


user_profile = {
    'question_get_content': 'SELECT id, title, body, creation_date FROM questions WHERE id = %s',
    'question_get_tags': 'SELECT tag_id FROM question_tags WHERE question_id = %s',
    'question_get_topics': """
        SELECT DISTINCT topic_id, weight FROM mls_question_topics
        WHERE question_id = %s ORDER BY weight DESC""",

    'get_topics': 'SELECT DISTINCT topic_id FROM mls_question_topics WHERE site_id = %s ORDER BY topic_id',

    'asked_qs': """
        SELECT id FROM questions
        WHERE removed IS NULL AND owner_id = %(user_id)s {since}""",

    'commented_qs': """
        SELECT question_id AS id FROM comments
        WHERE removed IS NULL AND question_id IS NOT NULL
        AND owner_id = %(user_id)s {since} UNION
        SELECT question_id AS id FROM answers
        WHERE removed IS NULL AND id IN (
            SELECT answer_id AS id FROM comments
            WHERE removed IS NULL AND question_id IS NULL
            AND owner_id = %(user_id)s {since})""",

    'favorited_qs': """
        SELECT q.id FROM user_favorites f
        LEFT JOIN questions q ON q.external_id = f.external_id
        WHERE q.removed IS NULL AND f.user_id = %(user_id)s {since} AND q.id IS NOT NULL""",

    'answer_query_base': 'SELECT question_id FROM answers WHERE removed IS NULL AND owner_id = %(user_id)s {since}',

    'feedback_query_base': """
        SELECT e.content_detail::int FROM evaluation_newsletters e
        LEFT JOIN newsletters n ON n.id = e.newsletter_id
        WHERE n.user_id = %(user_id)s
        AND e.content_type = 'question'
        AND e.user_response_type = '{fb}'
        AND e.user_response_detail::int {val}
        {{since}}
        UNION
        SELECT a.question_id FROM evaluation_newsletters e
        LEFT JOIN newsletters n ON n.id = e.newsletter_id
        LEFT JOIN answers a ON a.id = e.content_detail::int
        WHERE n.user_id = %(user_id)s
        AND e.content_type = 'answer'
        AND e.user_response_type = '{fb}'
        AND e.user_response_detail::int {val}
        {{since}}""",

    'community_asked_qs': 'SELECT id FROM questions WHERE removed IS NULL {since}',
    'community_answer_query_base': 'SELECT question_id FROM answers WHERE removed IS NULL {since}',
}


sections = {
    'hot-questions': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            {joins}
            WHERE q.site_id = %(site_id)s
            AND q.score > 3
            AND q.closed_date IS NULL
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            {where}
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",

    'useful-questions': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            LEFT JOIN answers a ON q.id = a.question_id
            {joins}
            WHERE q.site_id = %(site_id)s
            AND q.score > 3
            AND q.closed_date IS NULL
            AND a.question_id IS NOT NULL
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            {where}
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",

    'awaiting-answer': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            LEFT JOIN answers a ON q.id = a.question_id
            {joins}
            WHERE q.site_id = %(site_id)s
            AND q.score >= 0
            AND q.closed_date IS NULL
            AND a.question_id IS NULL
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            {where}
            ) x
        ORDER BY score ASC, creation_date DESC
        LIMIT 500""",

    'popular-unanswered': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.score, q.creation_date
            FROM questions q
            LEFT JOIN answers a ON q.id = a.question_id
            {joins}
            WHERE q.site_id = %(site_id)s
            AND q.score > 1
            AND q.closed_date IS NULL
            AND a.question_id IS NULL
            AND q.id NOT IN %(dupes)s
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            {where}
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",

    'highly-discussed-qs': """
        SELECT DISTINCT * FROM (
            SELECT q.id, q.comment_count, q.creation_date
            FROM questions q
            {joins}
            WHERE q.site_id = %(site_id)s
            AND q.score >= 0
            AND q.id NOT IN %(dupes)s
            AND q.comment_count > 3
            AND q.removed IS NULL
            AND q.creation_date > %(since)s
            {where}
            ) x
        ORDER BY comment_count DESC, creation_date DESC
        LIMIT 500""",

    'highly-discussed-as': """
        SELECT DISTINCT * FROM (
            SELECT a.id, a.comment_count, a.creation_date
            FROM answers a
            LEFT JOIN questions q ON q.id = a.question_id
            {joins}
            WHERE a.site_id = %(site_id)s
            AND a.score >= 0
            AND a.id NOT IN %(dupes)s
            AND a.comment_count > 3
            AND a.removed IS NULL
            AND a.creation_date > %(since)s
            {where}
            ) x
        ORDER BY comment_count DESC, creation_date DESC
        LIMIT 500""",

    'interesting-answers': """
        SELECT DISTINCT * FROM (
            SELECT a.id, a.score, a.creation_date
            FROM answers a
            LEFT JOIN questions q ON q.id = a.question_id
            {joins}
            WHERE a.site_id = %(site_id)s
            AND a.id NOT IN %(dupes)s
            AND a.score > 1
            AND a.removed IS NULL
            AND a.creation_date > %(since)s
            {where}
            ) x
        ORDER BY score DESC, creation_date DESC
        LIMIT 500""",
}


def build_section(section, mode='both'):
    if section not in sections:
        raise AttributeError('No such section')
    query_base = sections[section]
    joins, where = '', ''

    if mode == 'tags' or mode == 'both':
        joins += ' LEFT JOIN question_tags qt ON qt.question_id = q.id'
    if mode == 'topics' or mode == 'both':
        joins += ' LEFT JOIN mls_question_topics qto ON qto.question_id = q.id'

    if mode == 'tags':
        where = ' AND qt.tag_id = %(tags)s'
    elif mode == 'topics':
        where = ' AND qto.topic_id = %(topics)s'
    elif mode == 'both':
        where = ' AND (qt.tag_id IN %(tags)s OR qto.topic_id IN %(topics)s)'

    return query_base.format(joins=joins, where=where)
