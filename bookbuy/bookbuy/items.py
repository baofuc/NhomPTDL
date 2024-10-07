# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BookbuyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    Book_name = scrapy.Field()
    Author = scrapy.Field()
    Price = scrapy.Field()
    Market_Price = scrapy.Field()
    Status = scrapy.Field()
    Publisher = scrapy.Field() # add new
    Issuiers = scrapy.Field() # add new
    Publish_date = scrapy.Field() # add new
    Num_Page = scrapy.Field() # add new
    Weight = scrapy.Field() # add new
    Content = scrapy.Field() # add new
    pass
