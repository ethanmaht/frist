from functools import wraps
import jwt
from flask import g, request, Flask, current_app, jsonify, Response, make_response, Blueprint
import datetime
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity, get_jwt
import os
from app.users.model import Users
from ..users import model
# from .. import common
from app.auth.auths import Auth
from flask import render_template, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user, login_fresh, confirm_login, LoginManager
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms import ValidationError
from wtforms.validators import DataRequired, Length, Email, EqualTo, Regexp

from flask_cors import cross_origin
# Access-Control-Allow-Origin
# login_manager = LoginManager()


SECRET_KEY = 'TEST'

#
# @login_manager.user_loader
# def load_user(user_id):
#     user = model.Users()
#     return user


def init_api(app):
    @app.route('/login/', methods=['GET', 'POST'])
    def login():
        if request.method == 'POST':

            re_mas = make_response(index())
            re_mas.status = 200
            re_mas.headers['Authorization'] = 'token'
            return re_mas
        return render_template('login.html')

    @app.route('/index/', methods=['GET', 'POST'])
    def index():
        import time
        time.sleep(1)
        if request.method == 'POST':
            auth_header = request.headers
            print(auth_header)
        return render_template('index.html')

    @app.route('/logout/')
    def logout():
        logout_user()  # 登出用户
        flash('Goodbye.')
        return redirect(url_for('login'))  # 重定向回首页


def redirect_back(default='index', **kwargs):
    # for target in request.args.get('next'), request.referrer:
    #     if not target:
    #         continue
    #     if is_safe_url(target):
    #         return redirect(target)
    return redirect('/index/')


def true_return(data, msg):
    return {
        "status": True,
        "data": data,
        "msg": msg
    }


def false_return(data=None, msg=''):
    return {
        "status": False,
        "data": data,
        "msg": msg
    }


def encode_auth_token(user_id, login_time):
    """
    生成认证Token
    :param user_id: int
    :param login_time: int(timestamp)
    :return: string
    """
    try:
        payload = {
            'exp': datetime.datetime.utcnow() + datetime.timedelta(days=0, seconds=10),
            'iat': datetime.datetime.utcnow(),
            'iss': 'ken',
            'data': {
                'id': user_id,
                'login_time': login_time
            }
        }
        return jwt.encode(
            payload,
            SECRET_KEY,
            algorithm='HS256'
        )
    except Exception as e:
        return e
