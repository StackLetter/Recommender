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
    'term-vocab': 'term-vocab.pkl',
    'user-dir': 'users',
}

archive_dir = 'archive'
log_file = 'logs/app.log'
cron_log_file_daily = 'logs/cron-daily.log'
cron_log_file_weekly = 'logs/cron-weekly.log'

rollbar_token = ''
rollbar_env = 'recommender.production'

question_profile = SimpleNamespace()
question_profile.lda_threshold_percentile = 25

term_vocabulary_size = 150000
