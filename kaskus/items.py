# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class KaskusItem(Item):
    # define the fields for your item here like:
    # name = Field()
#    post_id = Field()
    post_content = Field()
    post_content_unparsed = Field()
    post_username = Field()
    post_userid = Field()
    post_time = Field()
    post_count = Field()



