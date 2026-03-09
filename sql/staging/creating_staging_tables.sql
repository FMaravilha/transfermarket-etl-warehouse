CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.players(
    
    player_id BIGINT PRIMARY KEY,
    name TEXT,
    date_of_birth DATE,
    position TEXT,
    sub_position TEXT,
    foot TEXT,
    height_in_cm INTEGER,
    market_value_in_eur BIGINT,
    highest_market_value_in_eur BIGINT,
    country_of_birth TEXT,
    city_of_birth TEXT,
    country_of_citizenship TEXT
);

CREATE TABLE IF NOT EXISTS staging.clubs(

    club_id BIGINT PRIMARY KEY,
    name TEXT,
    domestic_competition_id TEXT,
    total_market_value_in_eur BIGINT,
    squad_size INTEGER,
    average_age NUMERIC(5,2),
    national_team_players INTEGER,
    stadium_name TEXT,
    stadium_seats INTEGER,
    net_transfer_record TEXT,
    coach_name TEXT
);

CREATE TABLE IF NOT EXISTS staging.player_valuations(
    
    player_id BIGINT,
    valuation_date DATE,
    market_value_in_eur BIGINT,
    current_club_id BIGINT NULL,
    player_club_domestic_competition_id TEXT,
    PRIMARY KEY(player_id, valuation_date)
    
);