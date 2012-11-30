from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.spider import BaseSpider
from scrapy import log
from scrapy.http import *
from kaskus.items import KaskusItem
import MySQLdb, re, sys
import datetime
import db_base

'''
READ FIRST :
1. Kaskus (kaskus.co.id) is a forum, have thread and post
2. 1 thread, contains 20 post (max).
3. 1 Post Contains :
   post_id, post_date, post_content, post_content, 
   post_page_count, post_user_id, 
   post_user_name, post_user_join_date
4. Each xpath request, have a response count like the post
   example : 
   If in thread have 15 post, it returns each 15 too
   15 post_id, 15 post_date, 15 post_captdate, and so on..
5. And it no need to join string.

LOGIC :
1. Get list thread from db
   LOOK 'start_urls' (line 117)
2. Scraping with scrapy (follow url is exceptional)
3. Insert Scraped data to DB
'''

'''
MYSQL CONNECTION
'''
cursor = db_base.conn.cursor()

def remove_html(text):
    '''
    INPUT  : Raw Text (contains html is ok)
    OUTPUT : Fresh Text (No HTML, MYSQL escaped and Stripped)
    '''
    text = re.sub('<[^>]*>',' ',text.encode('utf-8')).strip()
    text = re.sub('\t','',text)
    text = re.sub('\n','',text)
    text = re.sub('\r','',text)
    text = MySQLdb.escape_string(text)
    return text

def encode_post(text):
    '''
    INPUT  : Raw Text (contains html is ok)
    OUTPUT : Fresh Text (MySQL Escaped and Stripped)
    '''
    text = text.encode('utf-8').strip()
    text = re.sub('\t','',text)
    text = re.sub('\n','',text)
    text = re.sub('\r','',text)    
    text = MySQLdb.escape_string(text)
    return text

def clear_text(text):
    if 'post' in text:
        text = text.replace('post','')
    if 'Join Date:' in text:
        text = text.replace('Join Date:','')
    if 'UserID:' in text:
        text = text.replace('UserID:','')
    if '#' in text:
        text = text.replace('#','').strip()
        pattern = re.compile(r'\s+')
        text = re.sub(pattern,'',text)
    return text

def find_page(url):
    if not 'page' in url:
        return 1
    else:
        return url.split('&')[1].split('=')[1]

def mysql_date_format(str_date, code):
    from_date_format = {
        'kaskus_post_date' : '%d-%m-%Y, %I:%M %p',
        'kaskus_join_date' : '%b %Y'
    }

    return_date_format = {
        'kaskus_post_date' : '%Y-%m-%d %H:%M:%S',
        'kaskus_join_date' : '%Y-%m-%d %H:%M:%S'
    }

    return datetime.datetime.strptime(str_date, from_date_format[code]).strftime(return_date_format[code])
    
def save_data(hxs, code ):
    '''
    INPUT  : HXS, String Code
    OUTPUT : List unicode from hxs
    '''
    xpath_kaskus ={
        'post_id'               : '//table[contains(@id,"post")]/@id',
        'post_date'             : '//table[contains(@id,"post") and @class = "tborder"]/tr/td[@class="thead" and not(@align="right")]',        
        'post_content'          : '//table[@class="tborder"]//td[@class="alt1" and contains(@id,"td_post")]',
        'post_page_count'       : '//table[contains(@id,"post") and @class = "tborder"]/tr/td[@class="thead" and @align="right"]/a/@name',
        'post_user_id'          : '//table[@class="tborder"]//td[@class="alt2"]/div[@class="smallfont"][2 and 3]/div[1]',
#        'post_user_name'        : '//table[@class="tborder"]//td[@class="alt2"]/div[contains(@id,"postmenu")]/a/text()',
        'post_user_name'        : '//a[@class="bigusername"]',
        'post_user_join_date'   : '//table[@class="tborder"]//td[@class="alt2"]/div[@class="smallfont"][2 and 3]/div[2]',

    }
    xpath = xpath_kaskus[code]
    return hxs.select(xpath).extract()
    

class KaskusSpiderSpider(CrawlSpider):
    name = 'kaskus_spider'
    allowed_domains = ['kaskus.co.id']
    start_urls = ['http://www.kaskus.co.id/showthread.php?t=16729256']
#    start_urls = ['http://www.kaskus.co.id/showthread.php?t=16494919']
    rules = (
        Rule(SgmlLinkExtractor(allow=('showthread\.php\?t=.*', )), callback='parse_item'),
    )

    def parse(self, response):
        # VARIABLE
        self.log('Main page %s' % response.url)
        base_url    = "http://www.kaskus.co.id/"
        hxs         = HtmlXPathSelector(response)
        thread_id   = response.url.split('?')[1].split('&')[0].split('=')[1]
        page_id     = find_page(response.url)
        counter     = 0
                    
        '''
        SAVE DATA
        '''
        save = {}
        save['post_id']             = save_data(hxs, 'post_id')
        save['post_date']           = save_data(hxs, 'post_date')
        save['post_content']        = save_data(hxs, 'post_content')
        save['post_page_count']     = save_data(hxs, 'post_page_count')
        save['post_user_id']        = save_data(hxs, 'post_user_id')
        save['post_user_name']      = save_data(hxs, 'post_user_name')
        save['post_user_join_date'] = save_data(hxs, 'post_user_join_date')
                
        for a, b, c, d, e, f, g, h in zip(save['post_id'], save['post_date'], save['post_content'], save['post_content'], save['post_page_count'], save['post_user_id'], save['post_user_name'], save['post_user_join_date']):
            #y = re.sub('<[^>]*>','',y.encode('utf-8')).strip()
            '''
            TEST PRINT
            print 'Post ID : %s | Post Date : %s \n Post Content : %s \n Post Content_UNPARSED : %s \nPost Page Count : %s | Post User ID : %s | Post User Name : %s | Post Join Date : %s \n' % ( remove_html( clear_text(a) ), remove_html(b), remove_html(c), encode_post(d), remove_html(e), remove_html( clear_text(f) ), remove_html(g), remove_html( clear_text(h) ) )
            '''
            sql_insert = "INSERT INTO _kaskus_content\
            (post_id, post_captdate, post_date, post_thread_id, post_page,\
            post_content, post_content_unparsed, post_page_count,\
            post_user_id, post_user_name, post_user_join_date)\
            VALUES ('%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') " % ( remove_html( clear_text(a) ), 'NOW()', 
            mysql_date_format( remove_html(b), 'kaskus_post_date'), thread_id, page_id, remove_html(c), encode_post(d), e.encode('utf-8'), 
            remove_html( clear_text(f) ), remove_html(g), 
            mysql_date_format( remove_html( clear_text(h) ), 'kaskus_join_date') )

#            print sql_insert
            try:
                cursor.execute(sql_insert)
                counter += 1
                print counter
            except:
                print "key duplicate"
        '''
        END SAVE DATA
        '''
        
        # For count thread < 10 (not have last)
        next_url_1 = hxs.select('//div[@class="pagenav"]/table/tr/td[@class="alt1"][last()]/a/@href').extract()
#        print "IF 1 NEXT URL"
#        print next_url_1
        try:
            if 'page=2' not in next_url_1[0]:
                # For count thread > 10 (have last)
                next_url_1 = hxs.select('//div[@class="pagenav"]/table/tr/td[@class="alt1"][last() -1 ]/a/@href').extract()
            thread = re.sub('(s=.*&t)','t',next_url_1[0])
            next_url = base_url + thread
    #        print next_url
            #print response.url
            
            yield Request(next_url, callback = self.parseNextUrl)
        except:
            pass
            
        '''
        Untuk tipe tabel innoDB, harus di commit
        '''
        #cursor.close()
        db_base.conn.commit()
            
    def parseNextUrl(self, response):
        self.log('Next page %s' % response.url)
        if (response.status == '404') or (response.status == '503'):
            sys.exit()
        base_url = "http://www.kaskus.co.id/"
        hxs = HtmlXPathSelector(response)
        thread_id   = response.url.split('?')[1].split('&')[0].split('=')[1]
        page_id     = find_page(response.url) 
        counter     = 0               

        '''
        SAVE DATA
        '''
        save = {}
        save['post_id']             = save_data(hxs, 'post_id')
        save['post_date']           = save_data(hxs, 'post_date')
        save['post_content']        = save_data(hxs, 'post_content')
        save['post_page_count']     = save_data(hxs, 'post_page_count')
        save['post_user_id']        = save_data(hxs, 'post_user_id')
        save['post_user_name']      = save_data(hxs, 'post_user_name')
        save['post_user_join_date'] = save_data(hxs, 'post_user_join_date')
                
        for a, b, c, d, e, f, g, h in zip(save['post_id'], save['post_date'], save['post_content'], save['post_content'], save['post_page_count'], save['post_user_id'], save['post_user_name'], save['post_user_join_date']):
            #y = re.sub('<[^>]*>','',y.encode('utf-8')).strip()
            '''
            TEST PRINT
            print 'Post ID : %s | Post Date : %s \n Post Content : %s \n Post Content_UNPARSED : %s \nPost Page Count : %s | Post User ID : %s | Post User Name : %s | Post Join Date : %s \n' % ( remove_html( clear_text(a) ), remove_html(b), remove_html(c), encode_post(d), remove_html(e), remove_html( clear_text(f) ), remove_html(g), remove_html( clear_text(h) ) )
            '''
            sql_insert = "INSERT INTO _kaskus_content\
            (post_id, post_captdate, post_date, post_thread_id, post_page,\
            post_content, post_content_unparsed, post_page_count,\
            post_user_id, post_user_name, post_user_join_date)\
            VALUES ('%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') " % ( remove_html( clear_text(a) ), 'NOW()', 
            mysql_date_format( remove_html(b), 'kaskus_post_date'), thread_id, page_id, remove_html(c), encode_post(d), e.encode('utf-8'), 
            remove_html( clear_text(f) ), remove_html(g), 
            mysql_date_format( remove_html( clear_text(h) ), 'kaskus_join_date') )

#            print sql_insert
            try:
                cursor.execute(sql_insert)
                counter += 1
                print counter
            except:
                print "key duplicate"
        '''
        END SAVE DATA
        '''
                    
        curr_page = response.url
        # For count thread < 10 (not have last)
        next_url_1 = hxs.select('//div[@class="pagenav"]/table/tr/td[@class="alt1"][last()]/a/@href').extract()
        try:
            temp = curr_page.split('&')[1]
            page = temp.split('=')[1]
            page = int(page) + 1
    #        print "IF 1 NEXT URL"
    #        print next_url_1
            if_string = 'page=%s' % page
            if if_string not in next_url_1[0]:
                print "IF STRING"
                print response.url
                print if_string
    #            print "NEXT URL <> 2"
                # For count thread > 10 (have last)
                next_url_1 = hxs.select('//div[@class="pagenav"]/table/tr/td[@class="alt1"][last() -1 ]/a/@href').extract()
    #            print next_url_1
            thread = re.sub('(s=.*&t)','t',next_url_1[0])
            next_url = base_url + thread
    #        print next_url
            #print response.url
            yield Request(next_url, callback = self.parseNextUrl)
        except:
            pass   
            
        db_base.conn.commit()
