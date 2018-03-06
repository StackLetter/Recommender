import itertools
import operator
from collections import Counter
from types import SimpleNamespace
from recommender import config, models, psql
import numpy
from sklearn.externals import joblib
from sklearn.feature_extraction.text import TfidfTransformer
import scipy.sparse as sparse
from pathlib import Path
from datetime import datetime
from cachetools import LFUCache, cached


@cached(cache=LFUCache(maxsize=100))
class QuestionProfile:

    def __init__(self, id):
        self.id = id
        with psql:
            cur = psql.cursor()
            cur.execute("SELECT id, title, body, creation_date FROM questions WHERE id = %s", (self.id,))
            _, self.title, self.body, self.creation_date = cur.fetchone()

    def tags(self):
        try:
            return self.__tags
        except AttributeError:
            with psql:
                cur = psql.cursor()
                cur.execute('SELECT tag_id FROM question_tags WHERE question_id = %s', (self.id,))
                self.__tags = [tag[0] for tag in cur]
                return self.__tags

    def topics(self):
        try:
            return self.__topics
        except AttributeError:
            with psql:
                cur = psql.cursor()
                cur.execute('SELECT topic_id, weight FROM mls_question_topics WHERE question_id = %s ORDER BY weight DESC', (self.id,))
                self.__topics = cur.fetchall()
                return self.__topics

    def terms(self):
        try:
            return self.__terms
        except AttributeError:
            vocab_model = models.load(models.MODEL_VOCAB)
            self.__terms = vocab_model.transform([models.process_question(self.title, self.body)])
            return self.__terms



class UserProfile:
    def __init__(self, id):
        self.id = id
        self.iterations = 0
        self.since = None
        self.interests = SimpleNamespace(tags=None, topics=None, terms=None, tfidf=None, total=0)
        self.expertise = SimpleNamespace(tags=None, topics=None, terms=None, tfidf=None, total=0)

    def save(self):
        model_dir = Path('.') / config.models['dir'] / config.models['user-dir']
        if not model_dir.exists():
            model_dir.mkdir(parents=True)
        model_file = model_dir / '{}.pkl'.format(self.id)
        return joblib.dump(self, model_file)

    @classmethod
    def load(cls, id):
        model_file = Path('.') / config.models['dir'] / config.models['user-dir'] / '{}.pkl'.format(id)
        if model_file.exists():
            return joblib.load(model_file)
        return cls(id)

    def _get_topics(self):
        try:
            return self.__topics
        except AttributeError:
            with psql:
                cur = psql.cursor()
                cur.execute('SELECT DISTINCT topic_id FROM mls_question_topics WHERE site_id = %s ORDER BY topic_id',
                            (config.site_id,))
                self.__topics = [topic[0] for topic in cur]
            return self.__topics

    def _get_question_profiles(self, question_query, since=None):
        params = {'user_id': self.id}
        since_query = 'AND created_at > %(since)s' if since else ''
        if since:
            params['since'] = since
        with psql:
            cur = psql.cursor()
            cur.execute(question_query.format(since=since_query), params)
            return [QuestionProfile(question[0]) for question in cur]

    def _get_qlists(self, since=None):
        # TODO add favorited questions
        asked_qs = self._get_question_profiles("""
            SELECT id FROM questions
            WHERE removed IS NULL AND owner_id = %(user_id)s {since}""", since)
        commented_qs = self._get_question_profiles("""
            SELECT question_id AS id FROM comments
            WHERE removed IS NULL AND question_id IS NOT NULL
            AND owner_id = %(user_id)s {since} UNION
            SELECT question_id AS id FROM answers
            WHERE removed IS NULL AND id IN (
                SELECT answer_id AS id FROM comments
                WHERE removed IS NULL AND question_id IS NULL
                AND owner_id = %(user_id)s {since})""", since)

        answer_query_base = 'SELECT question_id FROM answers WHERE removed IS NULL AND owner_id = %(user_id)s {since}'
        positive_as = self._get_question_profiles(answer_query_base + ' AND score >= 0', since)
        negative_as = self._get_question_profiles(answer_query_base + ' AND score < 0', since)
        accepted_as = self._get_question_profiles(answer_query_base + ' AND is_accepted', since)

        interests = [
            (asked_qs,      1.00),
            (commented_qs,  0.30),
        ]
        expertise = [
            (positive_as,   1.00),
            (negative_as,  -1.00),
            (accepted_as,   1.75),
            (commented_qs,  0.30),
        ]
        return interests, expertise

    def _sum_weighted_qlists(self, qlists):
        return sum(len(qlist) * abs(weight) for qlist, weight in qlists)

    def _sort_wlist(self, args):
        lst, weight = args
        return sorted(lst, key=lambda t: t[1], reverse=True), weight

    def _merge_wlists(self, old, new):
        sum_w = old[1] + new[1]
        res = Counter({k: v * old[1] for k, v in old[0]})
        for k, v in new[0]:
            res[k] += v * new[1] / sum_w
        return res.items(), sum_w

    def _get_tag_weights(self, weighted_qlists):
        tag_counts = Counter()
        for questions, weight in weighted_qlists:
            for q in questions:
                for tag in q.tags():
                    tag_counts[tag] += 1 * weight

        # Normalize values to 0..1
        total = sum(tag_counts.values())
        return [(tag, val/total) for tag, val in tag_counts.items()], total

    def _get_topic_weights(self, weighted_qlists):
        topic_list = self._get_topics()
        topic_distributions = {t: [] for t in topic_list}
        topic_weights = {t: [] for t in topic_list}
        question_count = 0
        for questions, q_weight in weighted_qlists:
            question_count += len(questions)
            for q in questions:
                for topic, weight in q.topics():
                    topic_distributions[topic].append(weight)

            # Add specific q_weights of questions to all topics
            for t in topic_list:
                topic_weights[t] += [abs(q_weight)] * (len(questions))

        # Fill missing Q-topic associations with zeros
        for t in topic_distributions:
            topic_distributions[t] += [0] * (question_count - len(topic_distributions[t]))

        # Calculate weighted average
        for t in topic_distributions:
            assert len(topic_distributions[t]) == len(topic_weights[t]), "Different lengths"
            try:
                topic_distributions[t] = numpy.average(topic_distributions[t], weights=topic_weights[t])
            except ZeroDivisionError:
                topic_distributions[t] = 0

        # Normalize values to 0..1
        total = sum(topic_distributions.values())
        return [(topic, weight/total) for topic, weight in topic_distributions.items()], total

    def _get_tf_matrix(self, weighted_qlists):
        # Flatten Q list
        weighted_questions = [(q, weight) for qlist, weight in weighted_qlists for q in qlist]
        # Create TF matrix while applying Q weights
        return sparse.vstack(itertools.starmap(operator.mul, ((q.terms(), w) for q, w in weighted_questions)), 'csr')

    def _calculate_tfidf(self, tf):
        return TfidfTransformer().fit_transform(tf)

    def train(self):
        int_qlists, exp_qlists = self._get_qlists()

        self.interests.tags = self._get_tag_weights(int_qlists)
        self.expertise.tags = self._get_tag_weights(exp_qlists)

        self.interests.topics = self._get_topic_weights(int_qlists)
        self.expertise.topics = self._get_topic_weights(exp_qlists)

        self.interests.terms = self._get_tf_matrix(int_qlists)
        self.expertise.terms = self._get_tf_matrix(exp_qlists)

        self.interests.tfidf = self._calculate_tfidf(self.interests.terms)
        self.expertise.tfidf = self._calculate_tfidf(self.expertise.terms)

        self.interests.total = self._sum_weighted_qlists(int_qlists)
        self.expertise.total = self._sum_weighted_qlists(exp_qlists)

        self.since = datetime.now()
        self.iterations += 1

    def retrain(self, since=None):

        if not since:
            since = self.since
        int_qlists, exp_qlists = self._get_qlists(since)

        interests_total = self._sum_weighted_qlists(int_qlists)
        expertise_total = self._sum_weighted_qlists(exp_qlists)

        # TODO apply exponential decay factor

        self.interests.tags = self._merge_wlists(self.interests.tags, self._get_tag_weights(int_qlists))
        self.expertise.tags = self._merge_wlists(self.expertise.tags, self._get_tag_weights(exp_qlists))

        self.interests.topics = self._merge_wlists(self.interests.topics, self._get_topic_weights(int_qlists))
        self.expertise.topics = self._merge_wlists(self.expertise.topics, self._get_topic_weights(exp_qlists))

        self.interests.terms = sparse.bmat([[self.interests.terms], [self._get_tf_matrix(int_qlists)]])
        self.expertise.terms = sparse.bmat([[self.expertise.terms], [self._get_tf_matrix(exp_qlists)]])

        self.interests.tfidf = self._calculate_tfidf(self.interests.terms)
        self.expertise.tfidf = self._calculate_tfidf(self.expertise.terms)

        self.interests.total += interests_total
        self.expertise.total += expertise_total

        self.since = datetime.now()
        self.iterations += 1

    def _get_profile_list(self, n, key, interests, expertise):
        interests_list = getattr(self.interests, key)
        expertise_list = getattr(self.expertise, key)

        if interests and expertise:
            res = self._merge_wlists(interests_list, expertise_list)
        elif interests:
            res = interests_list
        elif expertise:
            res = expertise_list
        else:
            return []

        sorted_list, _ = self._sort_wlist(res)
        length = len(sorted_list)
        max_index = min(length, abs(n)) if n != -1 else length
        return [t for t, _ in sorted_list[:max_index]]

    def get_tags(self, n, interests=True, expertise=True):
        return self._get_profile_list(n, 'tags', interests, expertise)

    def get_topics(self, n, interests=True, expertise=True):
        return self._get_profile_list(n, 'topics', interests, expertise)

    def match_questions(self, qlist, interests=True, expertise=True):
        q_index = [q.id for q in qlist]
        q_matrix = self._calculate_tfidf(sparse.vstack((q.terms() for q in qlist), 'csr'))

        if interests and expertise:
            u_matrix = sparse.bmat([[self.interests.tfidf], [self.expertise.tfidf]])
        elif interests:
            u_matrix = self.interests.tfidf
        elif expertise:
            u_matrix = self.expertise.tfidf
        else:
            return []

        # Calc mean values in user matrix along the '0' axis
        u_vector = u_matrix.mean(0)

        # Calc similarity (dot product) of Qs and user, return list of sorted Q-IDs
        product = u_vector * q_matrix.T
        sorted_list = sorted(zip(q_index, product.A[0]), key=lambda t: t[1], reverse=True)
        return sorted_list
