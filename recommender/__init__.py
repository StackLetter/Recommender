import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
from recommender import config
from sklearn.externals import joblib

from recommender import models, utils, profiles, db

# User profile threshold; If smaller, use community profile
profile_size_threshold = 5


class PersonalizedRecommender:

    def __init__(self, user, logger):
        self.user = user # type: profiles.UserProfile
        self.logger = logger
        logger.debug('Using PersonalizedRecommender')

    def recommend(self, rec_mode, section, since, dupes):
        content_type, profile_mode = rec_mode
        get_int = profile_mode == 'interests' or profile_mode == 'both'
        get_exp = profile_mode == 'expertise' or profile_mode == 'both'

        # Get top 5 tags and top 3 topics
        tags = self.user.get_tags(5, get_int, get_exp)
        topics = self.user.get_topics(3, get_int, get_exp)

        # Construct query based on section
        duplicates = dupes.get('question' if content_type == 'questions' else 'answer', [0])
        query = utils.queries.sections[section]
        query_params = {
            'site_id': config.site_id,
            'since': since,
            'dupes': tuple(duplicates if len(duplicates) > 0 else [0]),
            'tags': tuple(tags),
            'topics': tuple(topics),
        }

        # Run query
        with db.connection() as conn:
            cur = conn.cursor()
            cur.execute(query, query_params)
            results = [res[0] for res in cur]

        # If questions, match to user and return
        if content_type == 'questions':
            return self.user.match_questions([profiles.QuestionProfile(qid) for qid in results], get_int, get_exp)

        # If answers, we have some more work
        elif content_type == 'answers':
            # Construct question-answer index
            with db.connection() as conn:
                cur = conn.cursor()
                cur.execute(utils.queries.question_answer_index, {'answers': tuple(results)})
                qa_index = {qid: aid for qid, aid in cur}

            # Match questions to user
            qlist = [profiles.QuestionProfile(qid) for qid in qa_index.keys()]
            qmatches = self.user.match_questions(qlist, get_int, get_exp)

            # Return answers belonging to the matched questions
            return [(qa_index[qid], score) for qid, score in qmatches]
        # Something else? Better return nothing at all
        else:
            return []




def recommend(section, rec_mode, freq, uid, dupes, logger):
    _, profile_mode = rec_mode
    user = profiles.UserProfile.load(uid)

    if user.iterations == 0:
        logger.debug('User is untrained')
        user.train()
        user.save()
    else:
        logger.debug('User is on profile iteration #%d', user.iterations)

    if profile_mode == 'both':
        profile_size = user.interests.total + user.expertise.total
    else:
        profile_size = getattr(user, profile_mode).total

    logger.debug('Profile size: %.2f (mode: %s)', profile_size, profile_mode)

    if profile_size < profile_size_threshold:
        logger.debug('Insufficient profile size. Offload to trivial recommender')
        return fetch_trivial_recommendations(uid, section, freq, dupes) # TODO use community profile

    now = datetime.now()
    since = now - timedelta(days=8) if freq == 'w' else now - timedelta(days=2)

    rec = PersonalizedRecommender(user, logger)
    matches = rec.recommend(rec_mode, section, since, dupes)
    archive_matches(user, matches, section, freq)

    return [id for id, score in matches]


def archive_matches(user, matches, section, freq):
    archive_dir = Path('.') / config.archive_dir / '{:%Y-%m-%d}'.format(datetime.now()) / 'matches'
    if not archive_dir.exists():
        archive_dir.mkdir(parents=True)
    filepath = archive_dir / '{}_{}_{}.pkl'.format(freq, user.id, section)
    joblib.dump(matches, filepath)


def fetch_trivial_recommendations(uid, section, freq, dupes):
    endpoint_base = 'http://localhost:3000/recommendation/'
    endpoint_map = {
        'hot-questions':       'hot_questions',
        'useful-questions':    'useful_questions',
        'awaiting-answer':     'waiting_for_an_answer',
        'popular-unanswered':  'popular_unanswered',
        'highly-discussed-qs': 'highly_discussed_questions',
        'highly-discussed-as': 'highly_discussed_answers',
        'interesting-answers': 'answers_you_may_be_interested_in',
    }

    params = {
        'user_id': uid,
        'frequency': freq,
        'duplicates': json.dumps(dupes)
    }
    res = requests.get(endpoint_base + endpoint_map[section], params=params)

    if res.status_code == 200:
        return res.json()
    else:
        return []
