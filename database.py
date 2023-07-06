import mariadb
import sys
import random

import datetime


from items import Item

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
                database="mobile_ex",
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

    def init_tables(self):
        tables = ["video_keywords", "video", "keywords", "publisher", "institution", "child_friendly"]
        for table in tables:
            successful, result = self.execute(
                f"DROP TABLE IF EXISTS {table};"
            )
            if successful:
                qclose(successful, result)
            else:
                print("could not drop table ", table)

        self.create_keywords_table()
        self.create_child_friendly_table()
        self.create_institution_table()
        self.create_publisher_table()
        self.create_video_table()
        self.create_video_keywords_table()

        return True, "ok"

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
            "CREATE TABLE video("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "site_url VARCHAR(2083),"
            "video_url VARCHAR(2083),"
            "thumb_nail VARCHAR(2083),"
            "title VARCHAR(100),"
            "duration INT,"
            "category VARCHAR(100),"
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

    def create_keywords_table(self):

        successful, result = self.execute(
            "CREATE TABLE IF NOT EXISTS keywords("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "keyword VARCHAR(50));"
        )
        if not successful:
            return False, result

        return True, "ok"

    def create_publisher_table(self):

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

    def create_institution_table(self):

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

    def create_child_friendly_table(self):

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

    def create_video_keywords_table(self):
        successful, result = self.execute(
            "CREATE TABLE video_keywords("
            "id INT PRIMARY KEY AUTO_INCREMENT,"
            "video_id INT,"
            "keyword_id INT,"
            "FOREIGN KEY (video_id) REFERENCES video(id),"
            "FOREIGN KEY (keyword_id) REFERENCES keywords(id)"
            ");"
        )
        if not successful:
            return False, result

        qclose(successful, result)

        return True, "ok"

    def insert_video(self, video_item):

        self.insert_child_friendly(video_item.is_child_friendly)
        self.insert_institution(video_item.institution, video_item.institution_logo)
        self.insert_publisher(video_item.publisher)
        for keyword in video_item.keywords:
            if keyword is not None:
                self.insert_keyword(keyword)

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
                        # print(f"publisher_id: {publisher_id[0]}")
                    qclose(successful, result)

                # get id of institution
                successful, result = self.execute(
                    "SELECT id FROM institution "
                    f"WHERE institution_name = '{video_item.institution}'"
                )
                if successful:
                    if result is not None:
                        institution_id = result.fetchone()
                        # print(f"institution_id: {institution_id[0]}")
                    qclose(successful, result)

                # get id of child_friendly
                successful, result = self.execute(
                    "SELECT id FROM child_friendly "
                    f"WHERE status = {video_item.is_child_friendly}"
                )
                if successful:
                    if result is not None:
                        child_friendly_id = result.fetchone()
                        # print(f"child_friendly_id: {child_friendly_id[0]}")
                    qclose(successful, result)

                if publisher_id is None or \
                    institution_id is None or \
                    child_friendly_id is None:
                    return False, "insert"
                    

                successful, result = self.execute(
                    "INSERT INTO video("
                    "site_url,"
                    "video_url,"
                    "thumb_nail,"
                    "title,"
                    "duration,"
                    "category,"
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
                    f"'{video_item.duration}',"
                    f"'{video_item.category}',"
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

                # get video_id to insert into video_keywords table
                successful, result = self.execute(
                    "SELECT id from video "
                    f"WHERE title = '{video_item.title}';"
                )
                if successful:
                    if result is not None:
                        content = result.fetchone()
                        qclose(successful, result)
                        if content is not None:
                            self.insert_video_keywords(content[0], video_item.keywords)  # content[0] -> video_id
                    else:
                        qclose(successful, result)

            else:
                qclose(successful, result)
                return False, "exist"
        else:
            return False, result

        return True, "ok"

    def insert_video_keywords(self, video_id, keywords):

        for keyword in keywords:
            if keyword is not None:
                successful, result = self.execute(
                    "SELECT id FROM keywords "
                    f"WHERE keyword = '{keyword}';"
                )
                if successful:
                    if result is not None:
                        content = result.fetchone()
                        qclose(successful, result)
                        if content is not None:
                            keyword_id = content[0]
                            successful, result = self.execute(
                                "INSERT INTO video_keywords(video_id, keyword_id) "
                                f"VALUES('{video_id}', '{keyword_id}');"
                            )
                            if successful:
                                qclose(successful, result)

                    else:
                        qclose(successful, result)

    def insert_keyword(self, keyword):

        successful, result = self.execute(
            "SELECT * FROM keywords "
            f"WHERE keyword = '{keyword}';"
        )
        if successful:
            if result.fetchone() is None:
                qclose(successful, result)
                successful, result = self.execute(
                    f"INSERT INTO keywords(keyword)"
                    f"VALUES('{keyword}');"
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

    def debug_keywords_table(self):

        successful, result = self.execute(
            "SELECT * FROM keywords;"
        )
        if successful:
            if result is not None:
                print("keywords:")
                for r in result:
                    print(r)
            qclose(successful, result)

    def get_publisher(self, publisher_id):
        successful, result = self.execute(
            "SELECT * FROM publisher "
            f"WHERE id = {publisher_id};"
        )
        if successful:
            if result is not None:
                publisher = result.fetchone()
                if publisher is not None:
                    qclose(successful, result)
                    return publisher[1]
            qclose(successful, result)

        return None

    def get_institution(self, institution_id):
        successful, result = self.execute(
            "SELECT * FROM institution "
            f"WHERE id = {institution_id};"
        )
        if successful:
            if result is not None:
                institution = result.fetchone()
                if institution is not None:
                    qclose(successful, result)
                    return [institution[1], institution[2]]
            qclose(successful, result)

        return None, None


    def get_child_friendly(self, child_friendly_id):
#        print("$$$ ID : $$$", child_friendly_id)
        successful, result = self.execute(
            "SELECT * FROM child_friendly "
            f"WHERE id = {child_friendly_id};"
        )
        if successful:
            if result is not None:
                child_friendly = result.fetchone()
                if child_friendly is not None:
                    qclose(successful, result)
                    return child_friendly[1]
            qclose(successful, result)

        return None

    def get_keywords(self, id):
        successful, result = self.execute(
            "SELECT * FROM video_keywords "
            f"WHERE video_id = {id};"
        )
        keyword_ids = []
        if successful:
            if result is not None:
                keywords = result.fetchall()
                if keywords is not None:
                    for key in keywords:
                        keyword_ids.append(key[2])
            qclose(successful, result)


        keywords = []

        #print(keyword_ids)
        for key in keyword_ids:

            successful, result = self.execute(
                "SELECT keyword FROM keywords "
                f"WHERE id = {key}"
            )

            if successful:
                if result is not None:
                    keyword = result.fetchone()
                    if keyword is not None:
                        keywords.append(keyword[0])
                qclose(successful, result)

        if not keywords:
            return None

        return keywords

    def get_video_total_count(self):

        successful, result = self.execute(
            "SELECT COUNT(*) FROM video;"
        )

        number_videos = 0
        if successful:
            if result is not None:
                count = result.fetchone()
                if count is not None:
                    qclose(successful, result)
                    number_videos = count[0]
            qclose(successful, result)

        return number_videos


    # get the total number of videos
    def get_video_by_id(self, id):
        successful, result = self.execute(
            "SELECT * FROM video "
            f"WHERE id = {id};"
        )

        if successful:
            if result is not None:
                row = result.fetchone()
                if row is not None:
                    qclose(successful, result)
                    return row

            qclose(successful, result)

        return None

    def resolve_foreign_keys(self, video):
        # sql table indicies
        keywords = self.get_keywords(video[0])
        publisher = self.get_publisher(video[-3])
        institution, institution_logo = self.get_institution(video[-2])
        child_friendly = self.get_child_friendly(video[-1])
        return [keywords, publisher, institution, institution_logo, child_friendly]

    def get_random_videos_category(self, category, n):
        successful , result = self.execute(
            "SELECT * FROM video "
            f"WHERE category LIKE \"%{category}%\""
        )

        
        items = []
        if successful:
            if result is not None:
                videos = result.fetchall()
                if videos is not None:

                    total_videos = len(videos)
                    for i in range(n):  
                        rand = random.randint(1, total_videos - 1) 
                        video = videos[rand] # get random video of the set of all videos
                        item = Item()
                        keywords, publisher, institution, institution_logo, child_friendly = self.resolve_foreign_keys(video)
                        item.set_to_item(video, institution, institution_logo, publisher, child_friendly, keywords)
                        print(item)
                        items.append(item)
                    qclose(successful, result)
                    return items
            else:
                qclose(successful, result)
                return None
            qclose(successful, result)

        return None


    def get_random_videos(self, n):

        number_videos = self.get_video_total_count()

        # get 'n' random videos
        if number_videos > 0:
            items = []
            index = 0
            while index < n:
                rand = random.randint(1,number_videos)
                # video is a set
                video = self.get_video_by_id(rand)
                if video is not None: 
                    keywords, publisher, institution, institution_logo, child_friendly = self.resolve_foreign_keys(video)
                    item = Item()
                    item.set_to_item(video, institution, institution_logo,
                                     publisher, child_friendly, keywords)
                    items.append(item)
                    index += 1

            return items

        return None


if __name__ == '__main__':
    db = Database().instance()
    db.get_random_videos_category("doku", 1000)


#     db.create_video_keywords_table()
