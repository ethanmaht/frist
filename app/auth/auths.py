from flask import Flask, request, jsonify
import jwt, datetime
from app import config
from app import common
from app.users.model import Users
import time


class Auth:
    @staticmethod
    def encode_auth_token(user_id, login_time):
        try:
            payload = {
                'exp': datetime.datetime.utcnow() + datetime.timedelta(days=0, seconds=10),
                'iat': datetime.datetime.utcnow(),
                'iss': 'ken',
                'data': {
                    'id': user_id,
                    'login_time': login_time,
                }

            }
            return jwt.encode(payload, config.SECRET_KEY, algorithm='HS256')
        except Exception as e:
            return e

    @staticmethod
    def decode_auth_token(auth_token):
        try:
            payload = jwt.decode(auth_token, config.SECRET_KEY, option={'verify_exp': False})
            if 'data' in payload and 'id' in payload['data']:
                return payload
            else:
                raise jwt.InvalidTokenError
        except jwt.ExpiredSignatureError:
            return 'Token 过期'
        except jwt.InvalidTokenError:
            return 'Token 无效'

    def identify(self, requests):
        auth_header = requests.headers.get('Authorization')
        if auth_header:
            auth_tokenArr = auth_header.split(" ")
            if not auth_tokenArr or auth_tokenArr[0] != 'JWT' or len(auth_tokenArr) != 2:
                result = common.false_return('', '头信息不正确')
            else:
                auth_token = auth_tokenArr[1]
                payload = self.decode_auth_token(auth_token)
                if not isinstance(payload, str):
                    user = Users().check(payload['data']['id'])
                    result = common.true_return(user.userid, '请求成功')
                else:
                    result = common.false_return('', payload)
        else:
            result = common.false_return('', '头信息不正确')
        return result

    def authenticate(self, username, password):
        user_info = Users().query(username, password)
        if user_info:
            user_id = user_info
            login_time = int(time.time())
            token = self.encode_auth_token(user_id, login_time)
            # return jsonify(common.true_return(token.decode(), '登录成功'))
            print(token)
            return jsonify(common.true_return(token, '登录成功'))
        else:
            return jsonify('', '账号或密码错误')


