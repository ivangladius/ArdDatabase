import mariadb
import sys


def qclose(conn, cursor):
    if conn is not None:
        conn.close()
    if cursor is not None:
        cursor.close()


class Database:
    def __init__(self):
        self.pool = None

    def instance(self):
        if self.pool is None:
            self.pool = self.create_pool()
        return self

    def create_pool(self):
        try:
            self.pool = mariadb.ConnectionPool(
                host="0.0.0.0",
                port=3306,
                user="root",
                password="bitola",
                database="test1",
                pool_name="test_pool",
                pool_size=5,
                pool_validation_interval=250)
        except mariadb.PoolError as e:
            print(f"could not create pool: {e}")
            sys.exit(1)
        finally:
            return self.pool

    def execute(self, query):
        connection = None
        cursor = None
        try:
            if self.pool is not None:
                connection = self.pool.get_connection()
                connection.autocommit = False
                cursor = connection.cursor()
                cursor.execute(query)
        except mariadb.Error as e:
            print(f" error: {e}")
            connection.rollback()
            cursor.close()
            connection.close()

        connection.commit()
        return connection, cursor

    def create_video_table(self):
        conn, result = self.execute(
            "CREATE TABLE IF NOT EXISTS video("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "video_title VARCHAR(40),"
            "mp4 VARCHAR(4096));"
        )
        qclose(conn, result)

    def video_add(self, item):
        conn, result = self.execute(
            "INSERT INTO video(video_title, mp4)"
            f"VALUES('{item.title}', '{item.video_url}')"
        )
        qclose(conn, result)

    def video_show(self):
        conn, result = self.execute("SELECT * FROM video")
        if result is not None:
            for r in result:
                print(r)
        qclose(conn, result)

    def add_person(self, name):
        conn, result = self.execute(f"INSERT INTO person(name) VALUES('{name}')")
        qclose(conn, result)

    def get_person(self, name):
        conn, result = self.execute(f"SELECT * FROM person WHERE name LIKE '{name}'")
        if result is not None:
            for r in result:
                print(r)
        qclose(conn, result)

    def show(self):
        conn, result = self.execute("SELECT * FROM person;")
        if result is not None:
            for r in result:
                print(f"<{r[0]}, {r[1]}>")
        else:
            print("no results")

        qclose(conn, result)


if __name__ == '__main__':
    db = Database().instance()
    db.create_video_table()
    db.video_show()
