import json

import ciso8601


class Item:
    def __init__(self,
                 site_url = None,
                 video_url = None,
                 video_size = None, 
                 thumb_nail = None,
                 created = None,
                 institution = None,
                 institution_logo = None,
                 publisher = None,
                 title = None,
                 keywords = None,
                 duration = None,
                 category = None,
                 available_from = None,
                 available_to = None,
                 is_child_friendly = False
                 ):
        self.site_url = site_url
        self.video_url = video_url
        self.video_size = video_size
        self.thumb_nail = thumb_nail

        # convert to MariaDB TIMESTAMP
        self.created = created
        if self.created is not None:
            self.created = ciso8601.parse_datetime_as_naive(self.created)

        self.institution = institution
        self.institution_logo = institution_logo
        self.publisher = publisher

        self.title = title
        self.keywords = keywords
        self.duration = duration
        self.category = category

        # convert to MariaDB TIMESTAMP

        self.available_from = available_from
        if self.available_from is not None:
            self.available_from = ciso8601.parse_datetime_as_naive(self.available_from)

        self.available_to = available_to
        if self.available_to is not None:
            self.available_to = ciso8601.parse_datetime_as_naive(self.available_to)

        # convert to Bool for mariaDB
        # convert to Bool (integer 1 or 0)
        self.is_child_friendly = int(is_child_friendly)  # json.loads(is_child_content.lower())

    def set_to_item(self,
                    data,
                    institution,
                    institution_logo,
                    publisher,
                    child_friendly,
                    keywords):
        if data is not None:
            self.site_url = data[1]
            self.video_url = data[2]
#            self.video_size = data[3]
            self.thumb_nail = data[3]
            self.title = data[4]
            self.duration = data[5]
            self.category = data[6]
            self.created = data[7]
            self.available_from = data[8]
            self.available_to = data[9]
            self.institution_logo = institution_logo
            self.institution = institution
            self.keywords = keywords
            self.publisher = publisher
            self.child_friendly = child_friendly
        else:
            return False



    def __str__(self):
        return "###################\n" \
               f"site_url: {self.site_url}\n" \
               f"video_url: {self.video_url}\n" \
               f"vide_size: {self.video_size}\n" \
               f"thumb_nail: {self.thumb_nail}\n" \
               f"duration: {self.duration}\n" \
               f"created: {self.created}\n" \
               f"institution: {self.institution}\n" \
               f"institution_logo: {self.institution_logo}\n" \
               f"publisher: {self.publisher}\n" \
               f"title: {self.title}\n" \
               f"keywords: {self.keywords}\n" \
               f"duration: {self.duration}\n" \
               f"category: {self.category}\n" \
               f"available_from: {self.available_from}\n" \
               f"available_to: {self.available_to}\n" \
               f"is_child_content: {self.is_child_friendly}\n" \
               "###################\n"
