from flask import Flask, request


def create_app(config_name):
    app = Flask(__name__, template_folder='templates')
    app.config.from_object(config_name)

    @app.after_request
    def after_request(response):
        response.headers.add('Access-Contorol-Allow_Origin', '*')
        if request.method == 'OPTIONS':
            response.headers['Access-Contorol-Allow_Methods'] = 'DELETE, GET, POST, PUT'
            headers = request.headers.get('Access-Contorol-Allow_Headers')
            if headers:
                response.headers['Access-Contorol-Allow_Headers'] = headers
        return response

    from app.users.api import init_api
    init_api(app)
    return app
