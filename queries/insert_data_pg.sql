-- Table Artists
CREATE TABLE Artists (
    artist_id INT PRIMARY KEY,
    artist_name TEXT NOT NULL,
    link TEXT,
    followers INT,
    photo TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Albums
CREATE TABLE Albums (
    album_id INT PRIMARY KEY,
    album_title TEXT NOT NULL,
    artist_id INT,
    release_date DATE,
    link TEXT,
    cover TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (artist_id) REFERENCES Artists(artist_id)
);

-- Table Tracks
CREATE TABLE Tracks (
    track_id INT PRIMARY KEY,
    track_title TEXT NOT NULL,
    album_id INT,
    artist_id INT,
    duration INT,
    link TEXT,
    preview TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (album_id) REFERENCES Albums(album_id),
    FOREIGN KEY (artist_id) REFERENCES Artists(artist_id)
);

-- Table Genres
CREATE TABLE Genres (
    genre_id INT PRIMARY KEY,
    genre_name TEXT NOT NULL,
    picture_link TEXT,  -- Renamed from 'link' to 'picture_link'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Albums_Genres (Relationship between Albums and Genres)
CREATE TABLE Albums_Genres (
    album_id INT,
    genre_id INT,
    PRIMARY KEY (album_id, genre_id),
    FOREIGN KEY (album_id) REFERENCES Albums(album_id),
    FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
);

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for the Artists table
CREATE TRIGGER artists_update_trigger
BEFORE UPDATE ON Artists
FOR EACH ROW
WHEN (OLD.followers IS DISTINCT FROM NEW.followers OR OLD.photo IS DISTINCT FROM NEW.photo)
EXECUTE FUNCTION update_timestamp();

-- Trigger for the Albums table
CREATE TRIGGER albums_update_trigger
BEFORE UPDATE ON Albums
FOR EACH ROW
WHEN (OLD.cover IS DISTINCT FROM NEW.cover)
EXECUTE FUNCTION update_timestamp();

-- Trigger for the Tracks table
CREATE TRIGGER tracks_update_trigger
BEFORE UPDATE ON Tracks
FOR EACH ROW
WHEN (OLD.preview IS DISTINCT FROM NEW.preview)
EXECUTE FUNCTION update_timestamp();

-- Trigger for the Genres table
CREATE TRIGGER genres_update_trigger
BEFORE UPDATE ON Genres
FOR EACH ROW
WHEN (OLD.genre_name IS DISTINCT FROM NEW.genre_name OR OLD.picture_link IS DISTINCT FROM NEW.picture_link)
EXECUTE FUNCTION update_timestamp();


-- Alterar colunas para BIGINT
ALTER TABLE Artists
ALTER COLUMN artist_id TYPE BIGINT;

ALTER TABLE Albums
ALTER COLUMN album_id TYPE BIGINT,
ALTER COLUMN artist_id TYPE BIGINT;

ALTER TABLE Tracks
ALTER COLUMN track_id TYPE BIGINT,
ALTER COLUMN album_id TYPE BIGINT,
ALTER COLUMN artist_id TYPE BIGINT;

ALTER TABLE Genres
ALTER COLUMN genre_id TYPE BIGINT;

ALTER TABLE Albums_Genres
ALTER COLUMN album_id TYPE BIGINT,
ALTER COLUMN genre_id TYPE BIGINT;