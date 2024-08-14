create table if not exists sys.sources (
	id serial PRIMARY KEY,
	source_name text NOT NULL
);

create table if not exists sys.channels (
	id serial PRIMARY KEY,
	channel_url text NOT NULL,
	sourse_id int NOT NULL,
	CONSTRAINT fk_sourses FOREIGN KEY(sourse_id) REFERENCES sys.sources(id)
);

create table if not exists sys.find_topics (
	id serial PRIMARY KEY,
	topic_name text NOT null,
	summary_tags text not NULL
);

create table if not exists sys.find_topics_tags (
	id serial PRIMARY KEY,
	tags text NOT NULL,
	topic_id int NOT NULL,
	CONSTRAINT fk_find_topics FOREIGN KEY(topic_id) REFERENCES sys.find_topics(id)
);