import mariadb
import sys

import datetime


def qclose(connection, cursor):
    if connection is not None:
        connection.close()
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
                password="your_password",
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
        except mariadb.IntegrityError:
            connection.rollback()
            return False, "integrity"
        except mariadb.Error:
            connection.rollback()
            cursor.close()
            connection.close()
            return False, "connection"

        connection.commit()
        return connection, cursor

    # def add_child_friendly(self, status):
    #     status, result = self.execute(
    #         "INSERT INTO child_friendly(status)"
    #         f"VALUES('{status}');"
    #     )
    # if connection is False:
    #     if result

    def debug_child_friendly_table(self):
        conn, result = self.execute(
            "SELECT * FROM child_friendly;"
        )
        if result is not None:
            for r in result:
                print(r)
        qclose(conn, result)

    # def create_video_table(self):
    #     conn, result = self.execute(
    #         "CREATE TABLE IF NOT EXISTS video("
    #         "id INT PRIMARY KEY AUTO_INCREMENT,"
    #         "site_url VARCHAR(2083),"
    #         "video_url VARCHAR(2083),"
    #         "video_size INT,"
    #         "thumb_nail VARCHAR(2083),"
    #         "created DATETIME,"
    #         "institution VARCHAR(100),"
    #         "institution_logo VARCHAR(2083),"
    #         "publisher VARCHAR(100),"
    #         "title VARCHAR(100),"
    #         "duration INT,"
    #         "category VARCHAR(100),"
    #         "available_from DATETIME,"
    #         "available_to DATETIME,"
    #         "is_child_content BOOLEAN);"
    #     )
    #     qclose(conn, result)

    # def video_add(self, item):
    #     conn, result = self.execute(
    #         "INSERT INTO video"
    #         "(site_url,"
    #         "video_url,"
    #         "video_size,"
    #         "thumb_nail,"
    #         "created,"
    #         "institution,"
    #         "institution_logo,"
    #         "publisher,"
    #         "title,"
    #         "duration,"
    #         "category,"
    #         "available_from,"
    #         "available_to,"
    #         "is_child_content)"
    #         f"VALUES("
    #         f"'{item.site_url}',"
    #         f"'{item.video_url}',"
    #         f"'{item.video_size}',"
    #         f"'{item.thumb_nail}',"
    #         f"'{item.created}',"
    #         f"'{item.institution}',"
    #         f"'{item.institution_logo}',"
    #         f"'{item.publisher}',"
    #         f"'{item.title}',"
    #         f"'{item.duration}',"
    #         f"'{item.category}',"
    #         f"'{item.available_from}',"
    #         f"'{item.available_to}',"
    #         f"'{item.is_child_content}');"
    #     )
    #     qclose(conn, result)

    # def debug_video_table(self):
    #     conn, result = self.execute("SELECT * FROM video")
    #     if result is not None:
    #         for r in result:
    #             print(r)
    #     qclose(conn, result)


if __name__ == '__main__':
    db = Database().instance()
    db.add_child_friendly(0)
    db.add_child_friendly(0)
    db.add_child_friendly(0)
    db.add_child_friendly(1)
    db.add_child_friendly(1)
    db.debug_child_friendly_table()
