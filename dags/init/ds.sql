create table if not exists ds.news (
	id bigserial,
	title text,
	summary text,
	link text,
	published text,
	published_dt timestamp,
	author text,
	source_id text,
	channel_id text
);

create table if not exists ds.news_processed_entity (
	id  bigint,
	dt_processed timestamp,
	entity_name text,
	entity_label text,
	entity_similarity_to_topic float8,
	topic text
);

create table if not exists ds.news_processed_topic(
	id bigint,
	dt_processed timestamp,
	similarity_to_topic float8,
	similarity_to_summary_tags float8,
	topic text,
	summary_tags text
);
