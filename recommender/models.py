# from sklearn.feature_extraction.text import CountVectorizer
# from sklearn.decomposition import LatentDirichletAllocation
from sklearn.externals import joblib
from pathlib import Path
import re

from recommender import config

MODEL_LDA = 'lda-topics'
MODEL_VOCAB = 'term-vocab'

_models = {}
def load(model):
    if model in _models:
        return _models[model]

    _models[model] = joblib.load(Path('.') / config.models['dir'] / config.models[model])
    return _models[model]


def process_question(title, body):
    return "{} {}".format(title, re.sub(r'[\n\r\t ]+', ' ',re.sub(r'<[^<]+?>|&[\w\\]+;', '', body)))
