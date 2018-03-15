from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import numpy
from psycopg2.extras import execute_values

from recommender import models, config, db

model_lda = models.load(models.MODEL_LDA) # type: LatentDirichletAllocation
model_vocab = models.load(models.MODEL_VOCAB) # type: CountVectorizer

def get_question_topics(question):
    _, title, body = question

    # Calculate LDA topics distribution
    question_tf = model_vocab.transform([models.process_question(title, body)])
    question_topics = model_lda.transform(question_tf)[0]

    # Remove topics below threshold (25th percentile)
    threshold = numpy.percentile(question_topics, config.question_profile.lda_threshold_percentile)
    topics = list((weight, topic) for topic, weight in enumerate(question_topics) if weight > threshold)

    # Normalize weights
    total_weight = sum(weight for weight, _ in topics)
    return [(weight/total_weight, topic) for weight, topic in topics]


def persist_question_topics(question, topics):
    qid = question[0]
    with db.connection() as conn:
        execute_values(conn.cursor(), 'INSERT INTO mls_question_topics (question_id, topic_id, site_id, weight, created_at, updated_at) VALUES %s',
                       ((qid, topic, config.site_id, weight) for weight, topic in topics),
                       '(%s, %s, %s, %s, NOW(), NOW())')
