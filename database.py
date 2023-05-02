import mariadb
import sys

import datetime

from items import *


def qclose(connection, cursor):
    try:
        if connection is not None and connection is not False:
            connection.close()
            if cursor is not None:
                cursor.close()
    except mariadb.ProgrammingError:
        pass


def query_abort(connection, cursor, message):
    try:
        if connection is not None:
            connection.rollback()
            connection.close()
        elif cursor is not None:
            cursor.close()
    except mariadb.ProgrammingError:
        pass

    return False, message


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
                if connection is not None:
                    connection.autocommit = False
                    cursor = connection.cursor()
                    cursor.execute(query)
                else:
                    return False, "connection"
            else:
                return False, "unknown error"
        except mariadb.IntegrityError:
            return query_abort(connection, cursor, "integrity")
        except mariadb.Error:
            return query_abort(connection, cursor, "connection")

        if connection is not None and connection is not False:
            connection.commit()
            return connection, cursor
        else:
            return False, "unknown error"

    def update_database(self, items):
        for item in items:
            successful, result = self.execute(
                "SELECT * FROM video "
                f"WHERE title = '{item.title}'"
            )
            if successful:
                if result is not None:
                    row = result.fetchone()
                    if row is not None:
                        print(f"removing: {row[4]}")
                        qclose(successful, result)

                        successful, result = self.execute(
                            "DELETE FROM video "
                            f"WHERE title = '{item.title}';"
                        )
                        if not successful:
                            return False, "delete"
                        qclose(successful, result)

                        successful, result = self.execute(
                            "SELECT * FROM video "
                            f"WHERE title = '{item.title}'"
                        )
                        if successful:
                            if result is not None:
                                content = result.fetchone()
                                if content is None:
                                    print("removing successful!")
                                    qclose(successful, result)
                                else:
                                    qclose(successful, result)
                                    return False, "delete"
                    else:
                        return False, "delete"

                qclose(successful, result)
                return True, "ok"

        return False, "unknown error"

    def create_video_table(self):

        successful, result = self.execute(
            "DROP TABLE video;"
        )

        if not successful:
            return False, result

        qclose(successful, result)

        successful, result = self.execute(
            "CREATE TABLE video("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "site_url VARCHAR(2083),"
            "video_url VARCHAR(2083),"
            "thumb_nail VARCHAR(2083),"
            "title VARCHAR(100),"
            "created DATETIME,"
            "available_from DATETIME,"
            "available_to DATETIME,"
            "publisher_id INT,"
            "institution_id INT,"
            "child_friendly_id INT,"
            "FOREIGN KEY (publisher_id) REFERENCES publisher(id),"
            "FOREIGN KEY (institution_id) REFERENCES institution(id),"
            "FOREIGN KEY (child_friendly_id) REFERENCES child_friendly(id));"
        )
        if not successful:
            return False, result

        qclose(successful, result)

        return True, "ok"

    def insert_video(self, video_item):

        # self.create_child_friendly_table()
        # self.create_institution_table()
        # self.create_publisher_table()
        # self.create_video_table()
        self.insert_child_friendly(video_item.is_child_friendly)
        self.insert_institution(video_item.institution, video_item.institution_logo)
        self.insert_publisher(video_item.publisher)
        # self.debug_child_friendly_table()
        # self.debug_institution_table()
        # self.debug_publisher_table()

        # foreign keys to gather from db -> insert into video entry
        publisher_id = None
        institution_id = None
        child_friendly_id = None

        successful, result = self.execute(
            "SELECT * FROM video "
            f"WHERE title = '{video_item.title}';"
        )
        if successful:
            # add if video title was not already found
            if result.fetchone() is None:
                qclose(successful, result)
                # get id of publisher
                successful, result = self.execute(
                    "SELECT id FROM publisher "
                    f"WHERE publisher_name = '{video_item.publisher}'"
                )
                if successful:
                    if result is not None:
                        publisher_id = result.fetchone()
                        print(f"publisher_id: {publisher_id[0]}")
                    qclose(successful, result)

                # get id of institution
                successful, result = self.execute(
                    "SELECT id FROM institution "
                    f"WHERE institution_name = '{video_item.institution}'"
                )
                if successful:
                    if result is not None:
                        institution_id = result.fetchone()
                        print(f"institution_id: {institution_id[0]}")
                    qclose(successful, result)

                # get id of child_friendly
                successful, result = self.execute(
                    "SELECT id FROM child_friendly "
                    f"WHERE status = {video_item.is_child_friendly}"
                )
                if successful:
                    if result is not None:
                        child_friendly_id = result.fetchone()
                        print(f"child_friendly_id: {child_friendly_id[0]}")
                    qclose(successful, result)

                successful, result = self.execute(
                    "INSERT INTO video("
                    "site_url,"
                    "video_url,"
                    "thumb_nail,"
                    "title,"
                    "created,"
                    "available_from,"
                    "available_to,"
                    "publisher_id,"
                    "institution_id,"
                    "child_friendly_id)"
                    f"VALUES("
                    f"'{video_item.site_url}',"
                    f"'{video_item.video_url}',"
                    f"'{video_item.thumb_nail}',"
                    f"'{video_item.title}',"
                    f"'{video_item.created}',"
                    f"'{video_item.available_from}',"
                    f"'{video_item.available_to}',"
                    f"{publisher_id[0]},"  # id
                    f"{institution_id[0]},"  # id
                    f"{child_friendly_id[0]});"  # id
                )
                if not successful:
                    return False, result
                qclose(successful, result)
            else:
                qclose(successful, result)
                return False, "exist"
        else:
            return False, result

        self.debug_video_table()

        return True, "ok"

    def debug_video_table(self):

        successful, result = self.execute(
            "SELECT * FROM video;"
        )
        if successful:
            if result is not None:
                print("video:")
                for r in result:
                    print(r)
            qclose(successful, result)

    def create_institution_table(self):
        successful, result = self.execute(
            "DROP TABLE institution;"
        )

        if not successful:
            return False, result

        qclose(successful, result)

        successful, result = self.execute(
            "CREATE TABLE institution("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "institution_name VARCHAR(100),"
            "institution_logo VARCHAR(2083),"
            "UNIQUE(institution_name));"
        )
        if not successful:
            return False, result

        qclose(successful, result)

        return True, "ok"

    def create_publisher_table(self):

        successful, result = self.execute(
            "DROP TABLE publisher;"
        )

        if not successful:
            return False, result

        qclose(successful, result)

        successful, result = self.execute(
            "CREATE TABLE publisher("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "publisher_name VARCHAR(100),"
            "UNIQUE(publisher_name));"
        )
        if not successful:
            return False, result

        qclose(successful, result)

        return True, "ok"

    def insert_institution(self, institution_name, institution_logo):

        successful, result = self.execute(
            "SELECT * FROM institution "
            f"WHERE institution_name = '{institution_name}';"
        )
        if successful:
            if result.fetchone() is None:
                qclose(successful, result)
                successful, result = self.execute(
                    f"INSERT INTO institution(institution_name, institution_logo)"
                    f"VALUES('{institution_name}', '{institution_logo}');"
                )
                if not successful:
                    return False, result
                qclose(successful, result)
            else:
                qclose(successful, result)
                return False, "exist"
        else:
            return False, result

        return True, "ok"

    def debug_institution_table(self):

        successful, result = self.execute(
            "SELECT * FROM institution;"
        )
        if successful:
            if result is not None:
                print("institution:")
                for r in result:
                    print(r)
            qclose(successful, result)

    def insert_publisher(self, publisher):

        successful, result = self.execute(
            "SELECT * FROM publisher "
            f"WHERE publisher_name = '{publisher}';"
        )
        if successful:
            if result.fetchone() is None:
                qclose(successful, result)
                successful, result = self.execute(
                    f"INSERT INTO publisher(publisher_name)"
                    f"VALUES('{publisher}');"
                )
                if not successful:
                    return False, result
                qclose(successful, result)
            else:
                qclose(successful, result)
                return False, "exist"
        else:
            return False, result

        return True, "ok"

    def debug_publisher_table(self):

        successful, result = self.execute(
            "SELECT * FROM publisher;"
        )
        if successful:
            if result is not None:
                print("publisher:")
                for r in result:
                    print(r)
            qclose(successful, result)

    def create_child_friendly_table(self):

        successful, result = self.execute(
            "DROP TABLE child_friendly;"
        )

        if not successful:
            return False, result

        qclose(successful, result)

        successful, result = self.execute(
            "CREATE TABLE child_friendly("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "status BOOLEAN,"
            "UNIQUE(status));"
        )
        if not successful:
            return False, result

        qclose(successful, result)

        return True, "ok"

    def insert_child_friendly(self, status):

        successful, result = self.execute(
            "SELECT * FROM child_friendly "
            f"WHERE status = {status};"
        )
        if successful:
            if result.fetchone() is None:
                qclose(successful, result)
                successful, result = self.execute(
                    "INSERT INTO child_friendly(status)"
                    f"VALUES({status});"  # FIXME
                )
                if not successful:
                    return False, result
                qclose(successful, result)
            else:
                qclose(successful, result)
                return False, "exist"
        else:
            return False, result

        return True, "ok"

    def debug_child_friendly_table(self):
        successful, result = self.execute(
            "SELECT * FROM child_friendly;"
        )
        if successful:
            if result is not None:
                print("child_friendly:")
                for r in result:
                    print(r)
            qclose(successful, result)

#
# if __name__ == '__main__':
#     db = Database().instance()
#     db.create_publisher_table()
#     #  ignore duplicates
#     db.insert_publisher("ZDF")
#     db.insert_publisher("ZDF")
#     db.insert_publisher("ZDF")
#     db.insert_publisher("ARD")
#     db.insert_publisher("ARD")
#     db.insert_publisher("KiKA")
#     db.debug_publisher_table()
