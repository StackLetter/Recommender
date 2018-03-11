import flask, json
import recommender

def json_response(data, http_code=200):
    return flask.make_response(flask.jsonify(data), http_code)

app = flask.Flask(__name__)


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
        response = flask.make_response(flask.jsonify(obj), error.code)
        return response
    else:
        app.logger.error('Exception: %s', str(error))
        return flask.make_response(flask.jsonify({
            'status': 'error',
            'code': 500,
            'message': 'Internal Server Error',
        }), 500)


@app.route('/')
def default_route():
    return json_response({'status': 'ok'})


@app.route('/hot-questions/<string:frequency>/<int:user_id>')
def get_hot_questions(frequency, user_id):
    try:
        duplicates = json.loads(flask.request.args.get('duplicates', '{}'))
    except ValueError:
        return flask.abort(400)

    return json_response(recommender.recommend('hot-questions', 'interests', frequency, user_id, duplicates))


@app.route('/useful-questions/<string:frequency>/<int:user_id>')
def get_useful_questions(frequency, user_id):
    try:
        duplicates = json.loads(flask.request.args.get('duplicates', '{}'))
    except ValueError:
        return flask.abort(400)

    return json_response(recommender.recommend('useful-questions', 'interests', frequency, user_id, duplicates))


@app.route('/awaiting-answer/<string:frequency>/<int:user_id>')
def get_awaiting_answer(frequency, user_id):
    try:
        duplicates = json.loads(flask.request.args.get('duplicates', '{}'))
    except ValueError:
        return flask.abort(400)

    return json_response(recommender.recommend('awaiting-answer', 'expertise', frequency, user_id, duplicates))


@app.route('/popular-unanswered/<string:frequency>/<int:user_id>')
def get_popular_unanswered(frequency, user_id):
    try:
        duplicates = json.loads(flask.request.args.get('duplicates', '{}'))
    except ValueError:
        return flask.abort(400)

    return json_response(recommender.recommend('popular-unanswered', 'expertise', frequency, user_id, duplicates))


@app.route('/highly-discussed-qs/<string:frequency>/<int:user_id>')
def get_highly_discussed_qs(frequency, user_id):
    try:
        duplicates = json.loads(flask.request.args.get('duplicates', '{}'))
    except ValueError:
        return flask.abort(400)

    return json_response(recommender.recommend('highly-discussed-qs', 'interests', frequency, user_id, duplicates))
