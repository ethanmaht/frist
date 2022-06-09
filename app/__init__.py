from flask import Flask, request


def create_app():
    app = Flask(__name__)
    # app.config.from_object(config_name)

    return app
