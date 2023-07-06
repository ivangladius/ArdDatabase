
CREATE TABLE video(
    id INT PRIMARY KEY AUTO_INCREMENT,
    site_url VARCHAR(2083),
    video_url VARCHAR(2083),
    thumb_nail VARCHAR(2083),
    title VARCHAR(100),
    created DATETIME,
    available_from DATETIME,
    available_to DATETIME,
    publisher_id INT,
    institution_id INT,
    child_friendly_id INT,
    FOREIGN KEY (publisher_id) REFERENCES publisher(id),
    FOREIGN KEY (institution_id) REFERENCES institution(id),
    FOREIGN KEY (child_friendly_id) REFERENCES child_friendly(id)
);

CREATE TABLE institution(
    id INT PRIMARY KEY AUTO_INCREMENT,
    institution_name VARCHAR(100),
    institution_logo VARCHAR(2083),
    UNIQUE (institution_name)
);

CREATE TABLE publisher(
    id INT PRIMARY KEY AUTO_INCREMENT,
    publisher_name VARCHAR(100),
    UNIQUE (publisher_name)
);

CREATE TABLE child_friendly(
    id INT PRIMARY KEY AUTO_INCREMENT,
    status BOOLEAN,
    UNIQUE(status)
);

SELECT * FROM child_friendly;
SELECT * FROM institution;
SELECT * FROM video;
SELECT * FROM publisher;

TRUNCATE TABLE publisher;


DROP TABLE video;
DROP TABLE publisher;
DROP TABLE child_friendly;
DROP TABLE institution;

