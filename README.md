# news
Extract xmls in zip files from website, place the content of xmls in redis. The process is idempotent.

It places contents of each xml into a redis list NEWS_XML.

The topic_url is assumed to be a unique for the xml content so it is used to see if the content has already been stored in list.

the topic_url is stored as member of set where domain is extracted to be used as a key.

