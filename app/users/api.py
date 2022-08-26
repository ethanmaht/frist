from functools import wraps
import jwt
from flask import g, request, Flask, current_app, jsonify
import datetime
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
try:
    from urlparse import urlparse, urljoin
except ImportError:
    from urllib.parse import urlparse, urljoin

login_manager = LoginManager()


@login_manager.user_loader
def load_user(user_id):
    user = model.Users()
    return user


def init_api(app):
    @app.route('/login', methods=['GET', 'POST'])
    def register():
        # print(url_for('index'))
        form = LoginForm()
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            user = model.Users()
            if not username or not password:
                flash('username or password', 'info')
            else:
                if login_user(user):
                    flash('Login success.', 'info')
                    return redirect_back()
        return render_template('login.html')

    @app.route('/index', methods=['GET', 'POST'])
    @login_manager.user_loader
    def index():
        print(111111, )
        confirm_login()
        print(222222, )
        return render_template('index.html')


class LoginForm(FlaskForm):
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember me')
    submit = SubmitField('Log in')


def redirect_back(default='index', **kwargs):
    for target in request.args.get('next'), request.referrer:
        if not target:
            continue
        if is_safe_url(target):
            return redirect(target)
    # return redirect(url_for(default, **kwargs))
    return redirect('/index')


def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and \
        ref_url.netloc == test_url.netloc
