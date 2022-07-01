from functools import wraps
import jwt
from flask import g, request, Flask, current_app, jsonify
import datetime
import os
from app.users.model import Users
from .. import common
from app.auth.auths import Auth
from flask import render_template


def init_api(app):
    @app.route('/login', methods=['GET', 'POST'])
    def register():
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            print(request.headers.get('Authorization'))
            if not username or not password:
                return jsonify(common.false_return('', '用户名密码不能为空'))
            else:
                return Auth.authenticate(Auth, username, password)
        return render_template('login.html')

    @app.route('/user', methods=['GET', 'POST'])
    def user():
        print(111111, request.headers.get('Authorization'))
        result = Auth.identify(Auth, request)





