from flask import Flask, request


def create_app(config_name):
    app = Flask(__name__, template_folder='templates')
    app.config.from_object(config_name)

    @app.after_request
    def after_request(response):
        # response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Authorization')
        print(request.method)
        if request.method == 'OPTIONS':
            print(1111111111111)
            response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
            headers = request.headers.get('Access-Control-Allow-Headers')
            if headers:
                response.headers['Access-Control-Allow-Headers'] = headers
        return response

    from app.users.api import init_api
    init_api(app)
    return app
