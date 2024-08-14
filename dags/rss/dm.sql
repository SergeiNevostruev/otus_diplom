INSERT INTO dm.mart_news_entity
(
	id
	, title
	, summary
	, link
	, published
	, published_dt
	, author
	, source_id
	, channel_id
	, dt_processed
	, entity_name
	, entity_label
	, source_name
	, channel_url
	)
with entity as (
	select distinct id, dt_processed, entity_name, entity_label
	FROM ds.news_processed_entity
	where id > (select coalesce(max(id), 0) from dm.mart_news_entity )
),
join_tables as (
	SELECT
		dsn.id
		, dsn.title
		, dsn.summary
		, dsn.link
		, dsn.published
		, dsn.published_dt
		, dsn.author
		, dsn.source_id
		, dsn.channel_id
		, e.dt_processed
		, e.entity_name
		, e.entity_label
		, s.source_name
		, c.channel_url
	FROM ds.news dsn
	left join entity e on dsn.id=e.id
	left join sys.sources s on dsn.source_id::int=s.id
	left join sys.channels c on dsn.channel_id::int=c.id
	where dsn.id > (select coalesce(max(id), 0) from dm.mart_news_entity )
)
SELECT
	id
	, title
	, summary
	, link
	, published
	, published_dt
	, author
	, source_id::int
	, channel_id::int
	, dt_processed
	, entity_name
	, entity_label
	, source_name
	, channel_url
FROM join_tables
;

INSERT INTO dm.mart_news_topic
(
	id
	, title
	, summary
	, link
	, published
	, published_dt
	, author
	, source_id
	, channel_id
	, dt_processed
	, similarity_to_topic
	, similarity_to_summary_tags
	, topic
	, summary_tags
	, source_name
	, channel_url
)
with topic as (
	SELECT
		id
		, dt_processed
		, similarity_to_topic
		, similarity_to_summary_tags
		, topic
		, summary_tags
	FROM ds.news_processed_topic npt
	where id > (select coalesce(max(id), 0) from dm.mart_news_topic )
),
join_tables as (
	SELECT
		dsn.id
		, dsn.title
		, dsn.summary
		, dsn.link
		, dsn.published
		, dsn.published_dt
		, dsn.author
		, dsn.source_id
		, dsn.channel_id
		, t.dt_processed
		, t.similarity_to_topic
		, t.similarity_to_summary_tags
		, t.topic
		, t.summary_tags
		, s.source_name
		, c.channel_url
	FROM ds.news dsn
	left join topic t on dsn.id=t.id
	left join sys.sources s on dsn.source_id::int=s.id
	left join sys.channels c on dsn.channel_id::int=c.id
	where dsn.id > (select coalesce(max(id), 0) from dm.mart_news_topic )
)
SELECT
	id
	, title
	, summary
	, link
	, published
	, published_dt
	, author
	, source_id::int
	, channel_id::int
	, dt_processed
	, similarity_to_topic
	, similarity_to_summary_tags
	, topic
	, summary_tags
	, source_name
	, channel_url
FROM join_tables
;

