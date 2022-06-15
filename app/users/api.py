from functools import wraps
import jwt
from flask import g, request, Flask, current_app, jsonify
import datetime
import os
# import app


class Auth:

    def encode_auth_token(self):
        ...
