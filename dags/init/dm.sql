create table if not exists dm.mart_news_entity (
	id bigint,
	title text,
	summary text,
	link text,
	published text,
	published_dt timestamp,
	author text,
	source_id int,
	channel_id int,
	dt_processed timestamp,
	entity_name text,
	entity_label text,
	source_name text,
	channel_url text
);

create table if not exists dm.mart_news_topic (
	id bigint,
	title text,
	summary text,
	link text,
	published text,
	published_dt timestamp,
	author text,
	source_id int,
	channel_id int,
	dt_processed timestamp,
	similarity_to_topic float8,
	similarity_to_summary_tags float8,
	topic text,
	summary_tags text,
	source_name text,
	channel_url text
);