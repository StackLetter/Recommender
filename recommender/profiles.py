from collections import Counter, defaultdict
from types import SimpleNamespace
from recommender import config, models, psql, utils
import numpy

@utils.memoize
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



# TODO add lexical/term profile
class UserProfile:
    def __init__(self, id):
        self.id = id
        self.interests = SimpleNamespace(tags=None, topics=None, terms=None)
        self.expertise = SimpleNamespace(tags=None, topics=None, terms=None)

    def _get_question_profiles(self, question_query):
        with psql:
            cur = psql.cursor()
            cur.execute(question_query, {'user_id': self.id})
            return [QuestionProfile(question[0]) for question in cur]

    def _get_tag_weights(self, weighted_qlists):
        tag_counts = Counter()
        total = 0
        for questions, weight in weighted_qlists:
            total += len(questions)
            for q in questions:
                for tag in q.tags():
                    tag_counts[tag] += 1 * weight
        return [(tag, val/total) for tag, val in tag_counts.items()]

    def _get_topic_weights(self, weighted_qlists):
        topic_distributions = defaultdict(list)
        question_count = 0
        for questions, q_weight in weighted_qlists:
            question_count += len(questions)
            for q in questions:
                for topic, weight in q.topics():
                    topic_distributions[topic].append(weight * q_weight)

        for k in topic_distributions:
            topic_distributions[k] += [0] * (question_count - len(topic_distributions[k]))
            topic_distributions[k] = numpy.mean(topic_distributions[k])

        return list(topic_distributions.items())

    def train(self):
        # TODO add favorited questions
        asked_questions = self._get_question_profiles('SELECT id FROM questions WHERE removed IS NULL AND owner_id = %(user_id)s')
        commented_questions = self._get_question_profiles("""
            WITH commented_answers AS (SELECT answer_id AS id FROM comments WHERE removed IS NULL AND question_id IS NULL AND owner_id = %(user_id)s)
            SELECT question_id AS id FROM comments WHERE removed IS NULL AND question_id IS NOT NULL AND owner_id = %(user_id)s
            UNION SELECT question_id AS id FROM answers WHERE removed IS NULL AND id IN (SELECT id FROM commented_answers)""")

        answer_query_base = 'SELECT question_id FROM answers WHERE removed IS NULL AND owner_id = %(user_id)s'
        positive_answers = self._get_question_profiles(answer_query_base + ' AND score >= 0')
        negative_answers = self._get_question_profiles(answer_query_base + ' AND score < 0')
        accepted_answers = self._get_question_profiles(answer_query_base + ' AND is_accepted')

        interests_weighted_qlists = [
            (asked_questions, 1),
            (commented_questions, .3),
        ]

        expertise_weighted_qlists = [
            (positive_answers, 1),
            (negative_answers, -1),
            (accepted_answers, 1.5),
            (commented_questions, .3),
        ]

        self.interests.tags = self._get_tag_weights(interests_weighted_qlists)
        self.expertise.tags = self._get_tag_weights(expertise_weighted_qlists)

        self.interests.topics = self._get_topic_weights(interests_weighted_qlists)
        self.expertise.topics = self._get_topic_weights(expertise_weighted_qlists)


