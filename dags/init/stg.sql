create table if not exists stg.news (
	title text,
	summary text,
	link text,
	published text,
	published_dt timestamp,
	author text,
	source_id text,
	channel_id text
);