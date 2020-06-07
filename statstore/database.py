import pymysql

class SQL_DB(object):

    def __init__(self, host, db, user, pwd):
        self.host = host
        self.db = db
        self.user = user
        self.pwd = pwd
        self.connection = None
        self.connect()

    def connect(self):
        self.connection  = pymysql.connect(host=self.host,
            user=self.user,
            password=self.pwd,
            db=self.db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    def create_series(self, name, comment=""):
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO `series` (`name`, `comment`) VALUES (%s, %s)"
            cursor.execute(sql, (name, comment))
        self.connection.commit()
        return self.get_series_id(name)

    def get_series_id(self, name):
        with self.connection.cursor() as cursor:
            sql = "SELECT `id` FROM `series` WHERE `name`=%s"
            cursor.execute(sql, (name))
            result = cursor.fetchone()
            return result["id"]

    def push_value(self, series, datetime, value):
        if isinstance(series, str):
            series = self.get_series_id(series)
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO `data` (`datetime`, `series`, `value`) VALUES (%s, %s, %s)"
            cursor.execute(sql, (datetime, series, value))
        self.connection.commit()
