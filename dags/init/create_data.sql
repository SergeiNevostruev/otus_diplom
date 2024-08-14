INSERT INTO sys.sources
(source_name)
VALUES('rss');

INSERT INTO sys.channels
(channel_url, sourse_id)
VALUES
('http://www.gazeta.ru/export/rss/social_more.xml', 1),
('http://lenta.ru/rss/last24', 1),
('https://news.mail.ru/rss/', 1),
('http://tass.ru/rss/v2.xml', 1),
('http://news.rambler.ru/rss/incidents/', 1),
('http://news.rambler.ru/rss/world/', 1),
('http://www.ixbt.com/export/utf8/hardnews.rss', 1),
('http://russian.rt.com/rss/', 1)
;

INSERT INTO sys.find_topics
(topic_name, summary_tags)
VALUES
('Россия', 'Россия Росийская федерация'),
('Технологии', 'Россия ИТ цифровизация программирование исскуственный интеллект'),
('Электроэнергетика', 'Электроэнергетика россети электросети электрические сети электроснабжение сетевая компания Россети электричество')
;