import mariadb
import sys

import datetime


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

    def insert_video(self):


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
                    f"VALUES('{status}');"
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
                for r in result:
                    print(r)
            qclose(successful, result)


if __name__ == '__main__':
    db = Database().instance()
    db.create_child_friendly_table()
    db.create_publisher_table()
    db.create_institution_table()
    db.create_video_table()

