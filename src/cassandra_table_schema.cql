-- keyspace creation

DROP KEYSPACE IF EXISTS ks;

CREATE KEYSPACE IF NOT EXISTS dota_ks 
    WITH REPLICATION = {"class": "SingleRegionStrategy"};

-- table creation

-- my version

CREATE TABLE IF NOT EXISTS dota_ks.hero_data(
    match_id bigint,
    account_id bigint,
    duration int,
    year int,
    month int,
    day int,
    hero text,
    items set<text>,
    xp_per_min int,
    gold_per_min int,
    win boolean,
    PRIMARY KEY (match_id, account_id, (year, month, day))
);

-- AWS Keyspace version

CREATE TABLE "dota_ks"."hero_data"(
	"year" int,
	"month" int,
	"day" int,
	"match_id" bigint,
	"account_id" bigint,
	"duration" int,
	"hero" text,
	"items" set<text>,
	"xp_per_min" int,
	"gold_per_min" int,
	"win" boolean,
	PRIMARY KEY(("year", "month", "day"), "match_id", "account_id"))
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	}, 
	'point_in_time_recovery':{
		'status':'enabled'
	}, 
	'encryption_specification':{
		'encryption_type':'AWS_OWNED_KMS_KEY'
	}
} AND CLUSTERING ORDER BY("match_id" DESC, "account_id" DESC)