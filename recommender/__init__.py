import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
from recommender import config
from sklearn.externals import joblib

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

    def recommend(self, rec_mode, section, since, dupes):
        content_type, profile_mode = rec_mode
        get_int = profile_mode == 'interests' or profile_mode == 'both'
        get_exp = profile_mode == 'expertise' or profile_mode == 'both'

        # Get top 5 tags and top 3 topics
        tags = self.user.get_tags(5, get_int, get_exp)
        topics = self.user.get_topics(3, get_int, get_exp)

        # Construct query based on section
        query = utils.queries.sections[section]
        query_params = {
            'site_id': config.site_id,
            'since': since,
            'dupes': tuple(dupes.get('question', [0])),
            'tags': tuple(tags),
            'topics': tuple(topics),
        }

        # Run query
        with psql:
            cur = psql.cursor()
            cur.execute(query, query_params)
            results = [res[0] for res in cur]

        # If questions, match to user and return
        if content_type == 'questions':
            return self.user.match_questions([profiles.QuestionProfile(qid) for qid in results], get_int, get_exp)

        # If answers, we have some more work
        elif content_type == 'answers':
            # Construct question-answer index
            with psql:
                cur = psql.cursor()
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




def recommend(section, rec_mode, freq, uid, dupes):
    _, profile_mode = rec_mode
    user = profiles.UserProfile.load(uid)

    if user.iterations == 0:
        user.train()
        user.save()

    if profile_mode == 'both':
        profile_size = user.interests.total + user.expertise.total
    else:
        profile_size = getattr(user, profile_mode).total

    if profile_size < profile_size_threshold:
        return [] # TODO use community profile

    now = datetime.now()
    since = now - timedelta(days=24) if freq == 'w' else now - timedelta(days=3)
    #since = now - timedelta(days=8) if freq == 'w' else now - timedelta(days=2)

    rec = PersonalizedRecommender(user)
    matches = rec.recommend(rec_mode, section, since, dupes)
    archive_matches(user, matches, section, freq)

    return [id for id, score in matches]


def archive_matches(user, matches, section, freq):
    archive_dir = Path('.') / config.archive_dir / '{:%Y-%m-%d}'.format(datetime.now()) / 'matches'
    if not archive_dir.exists():
        archive_dir.mkdir(parents=True)
    filepath = archive_dir / '{}_{}_{}.pkl'.format(freq, user.id, section)
    joblib.dump(matches, filepath)
