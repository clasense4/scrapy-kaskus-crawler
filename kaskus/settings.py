# Scrapy settings for kaskus project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'kaskus'
BOT_VERSION = '1.0'

SPIDER_MODULES = ['kaskus.spiders']
NEWSPIDER_MODULE = 'kaskus.spiders'
USER_AGENT = '%s/%s' % (BOT_NAME, BOT_VERSION)

