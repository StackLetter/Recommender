from types import SimpleNamespace

site_id = 3

DB = SimpleNamespace()
DB.host = 'localhost'
DB.name = 'stackletter'
DB.user = 'postgres'
DB.password = ''

models = {
    'dir': 'models',
    'lda-topics': 'lda-topics.pkl',
    'term-vocab': 'term-vocab.pkl'
}

question_profile = SimpleNamespace()
question_profile.lda_threshold_percentile = 25