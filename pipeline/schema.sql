IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'alpha')
BEGIN
    EXEC('CREATE SCHEMA alpha');
END;

IF OBJECT_ID('alpha.recordings', 'U') IS NOT NULL DROP TABLE alpha.recordings;
IF OBJECT_ID('alpha.plants', 'U') IS NOT NULL DROP TABLE alpha.plants;
IF OBJECT_ID('alpha.botanists', 'U') IS NOT NULL DROP TABLE alpha.botanists;
IF OBJECT_ID('alpha.origin_locations', 'U') IS NOT NULL DROP TABLE alpha.origin_locations;
IF OBJECT_ID('alpha.countries', 'U') IS NOT NULL DROP TABLE alpha.countries;


CREATE TABLE alpha.countries (
    country_id   INT IDENTITY(1,1) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL
);


CREATE TABLE alpha.origin_locations (
    origin_location_id INT IDENTITY(1,1) PRIMARY KEY,
    city               VARCHAR(100) NOT NULL,
    country_id         INT NOT NULL,
    latitude           FLOAT NULL,
    longitude          FLOAT NULL,
    CONSTRAINT FK_origin_locations_countries
        FOREIGN KEY (country_id) REFERENCES alpha.countries(country_id)
);


CREATE TABLE alpha.botanists (
    botanist_id INT IDENTITY(1,1) PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) NULL,
    phone       VARCHAR(50) NULL
);


CREATE TABLE alpha.plants (
    plant_id           INT PRIMARY KEY,
    name               VARCHAR(100) NOT NULL,
    scientific_name    VARCHAR(150) NULL,
    origin_location_id INT NULL,
    CONSTRAINT FK_plants_origin_locations
        FOREIGN KEY (origin_location_id) REFERENCES alpha.origin_locations(origin_location_id)
);

CREATE TABLE alpha.recordings (
    recording_id   BIGINT IDENTITY(1,1) PRIMARY KEY,
    plant_id       INT NOT NULL,
    botanist_name  VARCHAR(100) NOT NULL,
    [timestamp]    DATETIME2(0) NOT NULL CONSTRAINT DF_recordings_timestamp DEFAULT SYSUTCDATETIME(),
    soil_moisture  FLOAT NULL,
    temperature    FLOAT NULL,
    last_watered   DATETIME2(0) NULL,
    CONSTRAINT FK_recordings_plants
        FOREIGN KEY (plant_id) REFERENCES alpha.plants(plant_id)
);
