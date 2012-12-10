# Scrapy Kaskus Crawler


This Crawler is create to crawl some [kaskus](http://kaskus.co.id) thread, 
like [this](http://www.kaskus.co.id/thread/50c3d3324f6ea10528000001).

Thread info, user info is saved to sql databases.

## Library used
1. [Scrapy](http://scrapy.org)
2. mysqldb

## How to Use :
1. Edit db_base.py change your database setting
2. Edit kaskus/settings.py, change your scrapy spider setting
3. Edit kaskus/spiders/new_kaskus_spider.py, change list of thread in this line:

        start_urls = ['http://www.kaskus.co.id/thread/509881921dd719d70e000015']
   
   Or You can do like this too
   
        start_urls = ['http://www.kaskus.co.id/thread/509881921dd719d70e000015', 'http://www.kaskus.co.id/thread/50c3d3324f6ea10528000001']
    
4. And start your crawler with this command

        scrapy crawl new_kaskus


## Notice    
The script is still sucks, not follow scrapy standards, use at your own risks.

mail me at clasense4[at]gmail[dot]com

[@clasense4](http://twitter.com/clasense4)