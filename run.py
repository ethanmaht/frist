from app import create_app
from flask_jwt_extended import JWTManager
from flask_cors import CORS
from flask_login import LoginManager
from flask import Flask

app = create_app('app.config')
# app = Flask(__name__)
# app.config['JWT_SECRET_KEY'] = 'jwt-secret-string'

# CORS(app, resources=r'/*')
CORS(app, supports_credentials=True)
# login_manager = LoginManager()
# login_manager.init_app(app)
# login_manager.login_view = 'login'
#
#
# @login_manager.user_loader
# def load_user(user_id):
#     return None


if __name__ == '__main__':
    app.run(debug=True)
