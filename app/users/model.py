from werkzeug.security import generate_password_hash


class Users:

    def __init__(self):
        self.username = 'a'
        self.password = '123'
        self.user_id = '1'
        self.last_time = '0'

    def query(self, username, password):
        if self.username == username and self.password == password:
            return ''
        else:
            return ''

    @property
    def is_active(self):
        return True

    @property
    def get_id(self):
        return call_id


def call_id():
    return '1'

