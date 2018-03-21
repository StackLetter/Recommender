import os
from datetime import datetime
import flask, json
import rollbar
import rollbar.contrib.flask
import recommender

def json_response(data, http_code=200):
    return flask.make_response(flask.jsonify(data), http_code)

app = flask.Flask(__name__)

@app.before_first_request
def init_rollbar():
    if app.debug:
        return

    rollbar.init(recommender.config.rollbar_token, recommender.config.rollbar_env,
                 root=os.path.dirname(os.path.realpath(__file__)),
                 allow_logging_basic_config=False)

    flask.got_request_exception.connect(rollbar.contrib.flask.report_exception, app)

@app.after_request
def set_response_headers(response):
    response.headers['Server'] = 'StackLetter-Recommender/1.0'
    return response

@app.errorhandler(400)  # Bad request
@app.errorhandler(401)  # Unauthorized
@app.errorhandler(403)  # Forbidden
@app.errorhandler(404)  # Not found
@app.errorhandler(405)  # Method not allowed
@app.errorhandler(500)  # Internal server error, unhandled exceptions
def error_http(error):
    from werkzeug import exceptions

    if isinstance(error, exceptions.HTTPException):
        app.logger.error('HTTP Error: #%d (%s) Request URL: %s', error.code, error.name, flask.request.url)
        obj = {
            'status': 'error',
            'code': error.code,
            'message': error.name,
        }
        if error.description:
            obj['reason'] = error.description
            app.logger.error('Reason: %s', error.description)
        response = json_response(obj, error.code)
        return response
    else:
        app.logger.error('Exception: %s', str(error))
        return json_response({
            'status': 'error',
            'code': 500,
            'message': 'Internal Server Error',
        }, 500)


@app.route('/')
def default_route():
    return json_response({'status': 'ok'})


@app.route('/recommend/<string:section>/')
def get_recommendations(section):
    section_mode_map = {
        'hot-questions':       ('questions', 'interests'),
        'useful-questions':    ('questions', 'interests'),
        'awaiting-answer':     ('questions', 'expertise'),
        'popular-unanswered':  ('questions', 'expertise'),
        'highly-discussed-qs': ('questions', 'both'),
        'highly-discussed-as': ('answers',   'both'),
        'interesting-answers': ('answers',   'both'),
    }
    if section not in section_mode_map.keys():
        return flask.abort(404)

    try:
        args = flask.request.args
        user_id = int(args['user_id'])
        frequency = args['frequency']
        duplicates = json.loads(args.get('duplicates', '{}'))
    except ValueError or KeyError:
        return flask.abort(400)

    app.logger.info('GET recommendations - user: %s, section: %s, freq: %s', user_id, section, frequency)

    # Choose recommender in A/B test based on date
    _, week, day = datetime.now().isocalendar()
    week_even = week % 2 == 0
    if (week_even and day > 3) or (not week_even and day <= 3):
        rec_type = 'diverse'
    else:
        rec_type = 'personalized'

    rec_mode = section_mode_map[section]
    results = recommender.recommend(rec_type, section, rec_mode, frequency, user_id, duplicates, logger=app.logger)

    app.logger.info('Returned %d results.', len(results))
    return json_response(results)


@app.teardown_appcontext
def close_db(_):
    app.logger.debug('Closing DB connection (App context teardown)')
    recommender.db.close()
