CREATE TABLE Brand (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(255) NOT NULL
);

CREATE TABLE Restaurant (
    restaurant_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    brand_id INT REFERENCES Brand(brand_id) ON DELETE CASCADE,
    cuisines VARCHAR(255),
    district VARCHAR(255),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    category VARCHAR(255),
    is_quality_merchant BOOLEAN
);

CREATE TABLE Review (
    review_id SERIAL PRIMARY KEY,
    restaurant_id INT REFERENCES Restaurant(restaurant_id) ON DELETE CASCADE,
    total_review INT,
    avg_review DOUBLE PRECISION
);
CREATE TABLE Menu (
    menu_key SERIAL PRIMARY KEY,
    restaurant_id INT REFERENCES Restaurant(restaurant_id) ON DELETE CASCADE,
    menu TEXT,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN
);
