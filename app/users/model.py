from werkzeug.security import generate_password_hash


class Users:

    def __init__(self):
        self.username = 'a'
        self.password = '123'
        self.user_id = 1
        self.last_time = 0

    def query(self, username, password):
        if self.username == username and self.password == password:
            return 1
        else:
            return 0

    def check(self, userid):
        if userid == 1:
            return 1
        else:
            return 0


