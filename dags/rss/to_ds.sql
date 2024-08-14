INSERT INTO ds.news
(title, summary, link, published, published_dt, author, source_id, channel_id)
SELECT title, summary, link, published, published_dt, author, source_id, channel_id
FROM stg.news sn
where sn.published_dt > (
	select coalesce(max(dsn.published_dt), to_timestamp(0)) from ds.news dsn
)
;