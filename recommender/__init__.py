import requests
import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path
from recommender import config
from sklearn.externals import joblib

from recommender import models, queries, profiles, db, utils

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
        query = queries.build_section(section)
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
                cur.execute(queries.question_answer_index, {'answers': tuple(results)})
                qa_index = {qid: aid for qid, aid in cur}

            # Match questions to user
            qlist = [profiles.QuestionProfile(qid) for qid in qa_index.keys()]
            qmatches = self.user.match_questions(qlist, get_int, get_exp)

            # Return answers belonging to the matched questions
            return [(qa_index[qid], score) for qid, score in qmatches]
        # Something else? Better return nothing at all
        else:
            return []




class DiverseRecommender:

    def __init__(self, user, logger):
        self.user = user # type: profiles.UserProfile
        self.logger = logger
        logger.debug('Using DiverseRecommender')

    def _get_buckets(self, n, interests, expertise):
        def split_half(lst):
            half = len(lst) // 2
            return lst[:half]

        def normalize(lst):
            total_weight = sum(w for _, w in lst)
            return [(val, w/total_weight) for val, w in lst]

        tags = normalize(split_half(self.user.get_tags(-1, interests, expertise, weights=True)))
        topics = normalize(split_half(self.user.get_topics(-1, interests, expertise, weights=True)))
        tags = [(('tags', val), weight) for val, weight in tags]
        topics = [(('topics', val), weight) for val, weight in topics]

        return normalize(random.sample(tags, math.ceil(n / 2)) + random.sample(topics, math.floor(n / 2)))

    def _get_recommendations(self, bucket, section, since, dupes, rec_mode, max_size):
        bucket_type, bucket_id = bucket
        content_type, profile_mode = rec_mode
        get_int = profile_mode == 'interests' or profile_mode == 'both'
        get_exp = profile_mode == 'expertise' or profile_mode == 'both'

        # Construct query based on section and bucket type
        duplicates = dupes.get('question' if content_type == 'questions' else 'answer', [0])
        query = queries.build_section(section, bucket_type)
        query_params = {
            'site_id': config.site_id,
            'since': since,
            'dupes': tuple(duplicates if len(duplicates) > 0 else [0]),
            bucket_type: bucket_id
        }

        # Run query
        with db.connection() as conn:
            cur = conn.cursor()
            cur.execute(query, query_params)
            results = [res[0] for res in cur]

        if not results:
            return []

        # If questions, match to user and return
        if content_type == 'questions':
            return self.user.match_questions([profiles.QuestionProfile(qid) for qid in results], get_int, get_exp)[:max_size]

        # If answers, we have some more work
        elif content_type == 'answers':
            # Construct question-answer index
            with db.connection() as conn:
                cur = conn.cursor()
                cur.execute(queries.question_answer_index, {'answers': tuple(results)})
                qa_index = {qid: aid for qid, aid in cur}

            # Match questions to user
            qlist = [profiles.QuestionProfile(qid) for qid in qa_index.keys()]
            qmatches = self.user.match_questions(qlist, get_int, get_exp)

            # Return answers belonging to the matched questions
            return [(qa_index[qid], score) for qid, score in qmatches[:max_size]]
        # Something else? Better return nothing at all
        else:
            return []

    def _select_from_bucket(self, buckets, rec_lists):
        bucket, bucket_weight = utils.weighted_choice(buckets)
        selected_list = rec_lists[bucket]
        item_count = math.ceil(len(selected_list) * bucket_weight)
        try:
            return random.choice(selected_list[:item_count]), bucket
        except IndexError:
            return None, None


    def recommend(self, rec_mode, section, since, dupes):
        rec_lst_size = 5

        content_type, profile_mode = rec_mode
        get_int = profile_mode == 'interests' or profile_mode == 'both'
        get_exp = profile_mode == 'expertise' or profile_mode == 'both'

        buckets = self._get_buckets(rec_lst_size, get_int, get_exp)
        rec_lists = {}
        self.logger.debug('Buckets: %d', len(buckets))
        for bucket, _ in buckets:
            self.logger.debug('- Bucket: %s - %d', bucket[0][0], bucket[0][1])
            rec_lists[bucket] = self._get_recommendations(bucket, section, since, dupes, rec_mode, rec_lst_size)

        self.logger.debug('Total buckets size: %d', sum(len(l) for l in rec_lists.values()))

        results = []
        archive = []
        while len(results) < rec_lst_size and sum(len(l) for l in rec_lists.values()) > 0:
            item, bucket = self._select_from_bucket(buckets, rec_lists)
            if item and item not in results:
                results.append(item)
                archive.append((item, bucket))
        return results, archive




def recommend(recommender_type, section, rec_mode, freq, uid, dupes, logger):
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
        logger.debug('Insufficient profile size. Using combined community profile')
        user = profiles.CommunityProfile.load()

    now = datetime.now()

    if recommender_type == 'diverse':
        since = now - timedelta(days=8) if freq == 'w' else now - timedelta(days=2)
        rec = DiverseRecommender(user, logger)
        matches, archive = rec.recommend(rec_mode, section, since, dupes)
        archive_matches(user, archive, section, 'div_' + freq)
    else:
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
