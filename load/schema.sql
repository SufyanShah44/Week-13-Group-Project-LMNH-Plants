DROP TABLE IF EXISTS recordings;
DROP TABLE IF EXISTS plants;
DROP TABLE IF EXISTS botanists;
DROP TABLE IF EXISTS origin_locations;
DROP TABLE IF EXISTS countries;

CREATE TABLE countries (
  country_id INT AUTO_INCREMENT PRIMARY KEY,
  country_name VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE origin_locations (
  origin_location_id INT AUTO_INCREMENT PRIMARY KEY,
  city VARCHAR(100) NOT NULL,
  country_id INT NOT NULL,
  latitude FLOAT,
  longitude FLOAT,
  FOREIGN KEY (country_id) REFERENCES countries(country_id)
) ENGINE=InnoDB;

CREATE TABLE botanists (
  botanist_id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(150),
  phone VARCHAR(50)
) ENGINE=InnoDB;

CREATE TABLE plants (
  plant_id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  scientific_name VARCHAR(150),
  origin_location_id INT,
  FOREIGN KEY (origin_location_id) REFERENCES origin_locations(origin_location_id)
) ENGINE=InnoDB;

CREATE TABLE recordings (
  recording_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  plant_id INT NOT NULL,
  botanist_id INT NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  soil_moisture FLOAT,
  temperature FLOAT,
  last_watered TIMESTAMP NULL,
  FOREIGN KEY (plant_id) REFERENCES plants(plant_id),
  FOREIGN KEY (botanist_id) REFERENCES botanists(botanist_id)
) ENGINE=InnoDB;