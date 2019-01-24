# scrapy-german-news
Collection of spiders crawling articles on major German news sites for the Scrapy application framework.
The spiders are usable as is, crawled data can be written to a PostgreSQL data base using the python psycopg2 package, MongoDB using pymongo, Kafka using kafka package or to a file as JSON.

## Table of Contents
* [Introduction](#introduction)
	* [Covered Sites](#covered_sites)
	* [Retrieved Data](#retrieved_data)
* [Installation](#installation)
	* [Requirements](#requirements)
* [Configuration](#configuration)
	* [Throttling](#throttling)
	* [User Agent](#useragent)
	* [Output](#output)
	* [Categories](#categories)
* [Usage](#usage)
	* [CLI](#cli)
	* [Daemon](#daemon)

<a name="introduction"/>
## Introduction
This is a collection of spiders for the Scrapy application framework.
The spiders cover major German news sites.
The repository includes a middleware for rotating user agents and two output pipelines:

<a name="covered_sites"/>
### Covered Sites
Following German news sites have spiders:
* [Frankfurter Allgemeine Zeitung](http://www.faz.net)
* [Focus Online](http://www.focus.de)
* [Spiegel Online](http://www.spiegel.de)
* [Stern](http://www.stern.de)
* [Sueddeutsche Zeitung](http://www.sueddeutsche.de)
* [Die Welt](http://www.welt.de)
* [Zeit Online](http://www.zeit.de)
* [n-tv](http://www.n-tv.de)
* [Handelsblatt](http://www.handelsblatt.com)
* [RP Online](http://www.rp-online.de)

Not the full sites are covered out of the box but only certain categories, see section [Categories](#categories) on how to modify them.

<a name="retrieved_data"/>
### Retrieved Data
Following information is extracted from these sites:
* URL
* Author
* Keywords
* Date published
* Date crawled
* Short description
* Full text
* Resource (or shortcut for crawled news site)

<a name="installation"/>
## Installation
The `requirements.txt` file contains a list of all required packages and can be installed using `pip`:
```bash
pip install -r requirements.txt
```
Alternatively the packages can be installed by hand:
```bash
pip install scrapy psycopg2
```
Scrapy is the framework for the spiders.
Psycopg2 is used for a pipeline to write the results to a PostgreSQL data base and can be skipped if the pipeline is commented out in `crawler/pipelines.py`.

<a name="requirements"/>
### Requirements
* Python 2.7 (Scrapy is not yet released for Python 3)
* Scrapy 1.0 ([website](http://scrapy.org/), [github](https://github.com/scrapy/scrapy), [doc](http://doc.scrapy.org/en/1.0/))

<a name="configuration"/>
## Configuration
The configuration is in `crawler/settings.py`.

<a name="throttling"/>
### Throttling
As to not overload the web server with requests, a download delay of 3 seconds is set.
Scrapy sends requests in an interval around this delay so it is not as easily recognizable as a crawler.
To remove the delay at your own risk, comment the line:
```python
DOWNLOAD_DELAY=3
```

<a name="useragent"/>
### User Agent
A middleware is included that allows for rotating user agents to make the crawler harder to identify as such.
The middleware is implemented in `crawler/middlewares.py` and can be set on spider to spider case.
To activate the rotating, uncomment the middleware:
```python
DOWNLOADER_MIDDLEWARES = {
    # Enable for rotationg user agents
    'crawler.middlewares.RotateUserAgentMiddleware': 110,
}
```
The used user agents are defined below as the most commonly used agents:
```python
USER_AGENT_CHOICES = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11) AppleWebKit/601.1.56 (KHTML, like Gecko) Version/9.0 Safari/601.1.56',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/601.2.7 (KHTML, like Gecko) Version/9.0.1 Safari/601.2.7',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko'
]
```
Change them or add your own as needed.

<a name="output"/>
### Output
Two different output methods are provided; writing to a PostgreSQL data base, MongoDB, Kafka and writing to a file as JSON.
The methods are implemented in `crawler/pipelines.py`
To choose a method comment/uncomment the lines in `crawler/settings.py` as needed:
```python
ITEM_PIPELINES = {
      'crawler.pipelines.KafkaPipeline':100
    # 'crawler.pipelines.MongoPipeline':200,
    # 'crawler.pipelines.PostgresPipeline': 300,
    # 'crawler.pipelines.JsonWriterPipeline': 800,
}
```

<a name="categories"/>
### Categories
Not the full sites are crawled but only certain categories (Mostly politics, economy, and general news).
To add categories or change them, modify the spider for the site.
Open the implementation of the spider in `crawler/spiders/` and look for the `rules` variable.
New categories can be added to the or case such as `$new_category` below.
```python
rules = (
	Rule(
		LinkExtractor(
			allow=(
				'(politik|gesellschaft|wirtschaft|$new_category).*\/index',
				'thema\/',
			)
		),
		follow=True
	),
	Rule(
		LinkExtractor(
			allow=('(politik|gesellschaft|wirtschaft|$new_category)(\/.+)*\/\d{4}-\d{1,2}\/.+'),
			deny=('-fs')
		),
	callback='parse_page',
),
```

<a name="usage"/>
## Usage

<a name="cli"/>
### CLI
The spiders can be run individually using the command line:
```bash
scrapy crawl $name
```
With `$name` being one of the following:

Site | URL | $name
------------ | ------------- | -------------
Frankfurter Allgemeine Zeitung | [http://www.faz.net](http://www.faz.net) | faz
Focus Online | [http://www.focus.de](http://www.focus.de) | focus
Spiegel Online | [http://www.spiegel.de](http://www.spiegel.de) | spiegel
Stern | [http://www.stern.de](http://www.stern.de) | stern
Sueddeutsche Zeitung | [http://www.sueddeutsche.de](http://www.sueddeutsche.de) | sz
Die Welt | [http://www.welt.de](http://www.welt.de) | welt
Zeit Online | [http://www.zeit.de](http://www.zeit.de) | zeit
n-tv | [http://www.n-tv.de](http://www.n-tv.de) | ntv
Handelsblatt | [http://www.handelsblatt.com](http://www.handelsblatt.com) | hb
RP Online | [http://ww.rp-online.de](http://www.rp-online.de) | rp

<a name="daemon"/>
### Daemon
For running multiple spiders at defined intervals as a daemon refer to [scrapyd](https://scrapyd.readthedocs.org/en/latest/overview.html)
