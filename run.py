from app import create_app
from flask_jwt_extended import JWTManager
from flask_login import LoginManager

app = create_app('app.config')
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


@login_manager.user_loader
def load_user(user_id):
    return None


if __name__ == '__main__':
    app.run()
