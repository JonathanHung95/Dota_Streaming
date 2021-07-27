CREATE TABLE match_data (
    -- individual player data
    account_id BIGINT,
    player_slot INT,
    hero_id INT,
    item_0 INT,
    item_1 INT,
    item_2 INT,
    item_3 INT,
    item_4 INT,
    item_5 INT,
    backpack_0 INT,
    backpack_1 INT,
    backpack_2 INT,
    item_neutral INT,
    leaver_status INT,
    gold_per_min INT,
    xp_per_min INT,
    -- match data
    radiant_win BOOLEAN,
    duration INT,
    start_time BIGINT,
    match_id BIGINT,
    cluster INT,
    lobby_type INT
);