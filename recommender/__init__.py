import psycopg2
from datetime import datetime, timedelta
from recommender import config

psql = psycopg2.connect(
    host=config.DB.host,
    database=config.DB.name,
    user=config.DB.user,
    password=config.DB.password,
)

from recommender import models, utils, profiles

# User profile threshold; If smaller, use community profile
profile_size_threshold = 5


class PersonalizedRecommender:

    def __init__(self, user):
        self.user = user # type: profiles.UserProfile

    def recommend(self, mode, section, since, dupes):
        get_int = mode == 'interests' or mode == 'both'
        get_exp = mode == 'expertise' or mode == 'both'

        # 1) get top 5 tags and top 3 topics
        tags = self.user.get_tags(5, get_int, get_exp)
        topics = self.user.get_topics(3, get_int, get_exp)

        # 2) Construct query based on section
        query = utils.queries.sections[section]
        query_params = {
            'site_id': config.site_id,
            'since': since,
            'dupes': tuple(dupes.get('question', [0])),
            'tags': tuple(tags),
            'topics': tuple(topics),
        }
        with psql:
            cur = psql.cursor()
            cur.execute(query, query_params)
            questions = [q[0] for q in cur]

        # Match questions to user
        matches = self.user.match_questions([profiles.QuestionProfile(qid) for qid in questions], get_int, get_exp)
        return [qid for qid, score in matches]


def recommend(section, profile_mode, freq, uid, dupes):
    user = profiles.UserProfile.load(uid)
    profile_size = getattr(user, profile_mode).total

    if user.iterations == 0:
        user.train()


    if profile_size < profile_size_threshold:
        return [] # TODO use community profile

    now = datetime.now()
    since = now - timedelta(days=24) if freq == 'w' else now - timedelta(days=3)
    #since = now - timedelta(days=8) if freq == 'w' else now - timedelta(days=2)

    rec = PersonalizedRecommender(user)
    return rec.recommend(profile_mode, section, since, dupes)
