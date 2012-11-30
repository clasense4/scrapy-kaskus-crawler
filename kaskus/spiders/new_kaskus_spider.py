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
import unicodedata

'''
READ FIRST :
1. New Kaskus with new layout (kaskus.co.id) is a forum, have thread and post
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
6. New Kaskus is much simpler than old kaskus

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
#    text = text.encode('ascii','ignore')
    try :
#        text = text.encode('ascii','replace')
        text = unicodedata.normalize('NFKD', text).encode('ascii','ignore')
    except:
        print '\nENCODE ERROR Remove HTML\n' + text
    text = text.strip()
    text = re.sub('<[^>]*>',' ',text)
    text = re.sub('\t','',text)
    text = re.sub('\n','',text)
    text = re.sub('\r','',text)
    try:
        text = MySQLdb.escape_string(text)
    except:
#        hash = hashlib.sha224(text).hexdigest()
        print '\nESCAPE ERROR Remove HTML\n' + text

    return text

def encode_post(text):
    '''
    INPUT  : Raw Text (contains html is ok)
    OUTPUT : Fresh Text (MySQL Escaped and Stripped)
    '''
#    text = text.encode('ascii','ignore').strip()
    try :
        text = text.encode('ascii','replace')
    except:
        print '\nDECODE ERROR Encode Post\n' + text
    text = text.strip()
    text = re.sub('\t','',text)
    text = re.sub('\n','',text)
    text = re.sub('\r','',text)    
    try:
        text = MySQLdb.escape_string(text)
    except:
#        hash = hashlib.sha224(text).hexdigest()
        print '\nESCAPE ERROR ENCODE Post\n' + text
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
    if ': ' in text:
        text = text.replace(': ','').strip()
    return text

def find_page(url):
    sp = url.split('/')
    if (len(sp) < 7):
        return 1
    else:
    #http://www.kaskus.co.id/thread/000000000000000016736400/macam-macam-permen-unik-di-dunia-pic/2
        return url.split('/')[-1]

def mysql_date_format(str_date, code):
    from_date_format = {
        'kaskus_post_date' : '%d-%m-%Y %H:%M',
        'kaskus_join_date' : '%d-%m-%Y'
    }

    return_date_format = {
        'kaskus_post_date' : '%Y-%m-%d %H:%M:%S',
        'kaskus_join_date' : '%Y-%m-%d %H:%M:%S'
    }
    # Prevent like this
    # Today 13:40
    if 'Today' in str_date:
        today = datetime.datetime.today()
        str_date = str_date.replace('Today',today.strftime('%d-%m-%Y'))
    elif 'Yesterday' in str_date:
        today = datetime.datetime.today()    
        yesterday = today - datetime.timedelta(1)
        str_date = str_date.replace('Yesterday',yesterday.strftime('%d-%m-%Y'))
        
    return datetime.datetime.strptime(str_date, from_date_format[code]).strftime(return_date_format[code])

def update_table_rss(url):
    sqlupdate = "update _kaskus_rss set rss_parsed = '1' where rss_link = '%s'" % (url)
    if cursor.execute(sqlupdate) :
        return True

def update_table_rss_redirect(red_url, res_url):
    '''
    Example url
    #    http://www.kaskus.co.id/showthread.php?s=bba2be2401e0d7db0662af7b6bd62677&t=16803515
    '''
    if 's=' in res_url:
        fix_url = re.sub('(s=.*&t)','t',res_url)
        print "IT REDIRECT"
        sqlupdate = "update _kaskus_rss set rss_redirect_url = '%s' where rss_link = '%s'" % (fix_url, red_url)
    else:
        sqlupdate = "update _kaskus_rss set rss_redirect_url = '%s' where rss_link = '%s'" % (red_url, res_url)

    if cursor.execute(sqlupdate) :
        return True

def update_table_rss_error_redirect(url):
    sqlupdate = "update _kaskus_rss set rss_parsed = 1 where rss_parsed = 0 and rss_redirect_url = '%s' " % (url)
    if cursor.execute(sqlupdate) :
        return True
            
def save_data(hxs, code ):
    '''
    INPUT  : HXS, String Code
    OUTPUT : List unicode from hxs
    '''
    xpath_kaskus ={
        'post_id'               : '//div[@class="row"][@id]/@id',
        'post_date'             : '//time/text()',        
        'post_content'          : '//div[@class="entry"]',
        'post_page_count'       : '//div[@class="permalink"]/a/text()',
        'post_user_id'          : '//div[@class="meta"]/div[1]/text()',
        'post_user_name'        : '//div[@class="user-details"]/a/text()',
        'post_user_join_date'   : '//div[@class="meta"]/div[2]/text()',
        'post_user_jobtitle'    : '//div[@class="title"]',

    }
    xpath = xpath_kaskus[code]
    if (code == 'post_user_name'):
        old_list = hxs.select(xpath).extract()
        new_list = [ x for x in old_list if x != u' ']
        return new_list
    else:
        return hxs.select(xpath).extract()
    

class KaskusSpiderSpider(CrawlSpider):
    name = 'new_kaskus_spider'
    allowed_domains = ['kaskus.co.id']
#    start_urls = ['http://www.kaskus.co.id/thread/509881921dd719d70e000015/10-film-yang-bisa-membuat-para-pria-menangis/3']
    start_urls = ['http://www.kaskus.co.id/thread/509881921dd719d70e000015']

#    start_urls = ['http://www.kaskus.co.id/showthread.php?t=16494919']
#    rules = (
#        Rule(SgmlLinkExtractor(allow=('showthread\.php\?t=.*', )), callback='parse_item'),
#    )

    def parse(self, response):
        # VARIABLE
        self.log('Main page %s' % response.url)
        base_url    = "http://www.kaskus.co.id"
        hxs         = HtmlXPathSelector(response)
        thread_id   = response.url.split('/')[4]
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
        save['post_user_jobtitle']  = save_data(hxs, 'post_user_jobtitle')
                
        for a, b, c, d, e, f, g, h, i in zip(save['post_id'], save['post_date'], save['post_content'], save['post_content'], save['post_page_count'], save['post_user_id'], save['post_user_name'], save['post_user_join_date'], save['post_user_jobtitle']):
            #y = re.sub('<[^>]*>','',y.encode('utf-8')).strip()
            
#            TEST PRINT
#            print 'Post ID = %s | Post Date = %s \n Post Content = %s \n Post Content_UNPARSED = %s \nPost Page Count = %s | Post User ID = %s | Post User Name = %s | Post Join Date = %s | Post Job Title = %s \n' % ( remove_html( clear_text(a) ), remove_html(b), remove_html(c), encode_post(d), remove_html(e), remove_html( clear_text(f) ), remove_html(g), remove_html( clear_text(h) ), remove_html( clear_text(i) ) )
            
            sql_insert = "\n\n\t\t\t\t\tINSERT INTO _kaskus_content\
            (post_id, \
            post_captdate, \
            post_date, \
            post_thread_id, \
            post_page,\
            post_content, \
            post_content_unparsed, \
            post_page_count,\
            post_user_id, \
            post_user_name, \
            post_user_join_date, \
            post_user_jobtitle)\
            VALUES \
            ('%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') " % ( remove_html( clear_text(a) ), 'NOW()', 
            mysql_date_format( remove_html(b), 'kaskus_post_date'), thread_id, page_id, 
            remove_html(c), 
            encode_post(d), 
            clear_text(e), 
            remove_html( clear_text(f) ), 
            remove_html(g), 
            mysql_date_format( remove_html( clear_text(h) ), 'kaskus_join_date'),
            remove_html( clear_text(i) ) )

#            print sql_insert
            try:
                cursor.execute(sql_insert)
                counter += 1
                sys.stdout.write(str(counter)+' ')
            except:
                print "key duplicate"
                print sql_insert
        '''
        END SAVE DATA
        '''
        
        # For count thread < 10 (not have last)
        next_url_1 = hxs.select('//div[@class="pagenav"]/table/tr/td[@class="alt1"][last()]/a/@href').extract()
        try:
            next_url_1 = hxs.select('//div[@class="pagination"]/ul/ul/li[last()-1]/a/@href').extract()
            next_url = base_url + next_url_1[0]
            print next_url
            print response.url
            
#            yield Request(next_url, callback = self.parseNextUrl)
        except:
            pass
            
        '''
        For innoDB tipe tabel, must commit
        '''
        db_base.conn.commit()
            
    def parseNextUrl(self, response):
        # VARIABLE
        self.log('Next page %s' % response.url)
        base_url    = "http://www.kaskus.co.id"
        hxs         = HtmlXPathSelector(response)
        thread_id   = response.url.split('/')[4]
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
        save['post_user_jobtitle']  = save_data(hxs, 'post_user_jobtitle')
                
        for a, b, c, d, e, f, g, h, i in zip(save['post_id'], save['post_date'], save['post_content'], save['post_content'], save['post_page_count'], save['post_user_id'], save['post_user_name'], save['post_user_join_date'], save['post_user_jobtitle']):
            #y = re.sub('<[^>]*>','',y.encode('utf-8')).strip()
            
#            TEST PRINT
#            print 'Post ID = %s | Post Date = %s \n Post Content = %s \n Post Content_UNPARSED = %s \nPost Page Count = %s | Post User ID = %s | Post User Name = %s | Post Join Date = %s | Post Job Title = %s \n' % ( remove_html( clear_text(a) ), remove_html(b), remove_html(c), encode_post(d), remove_html(e), remove_html( clear_text(f) ), remove_html(g), remove_html( clear_text(h) ), remove_html( clear_text(i) ) )
            
            sql_insert = "\n\n\t\t\t\t\tINSERT INTO _kaskus_content\
            (post_id, post_captdate, post_date, post_thread_id, post_page,\
            post_content, post_content_unparsed, post_page_count,\
            post_user_id, post_user_name, post_user_join_date, post_user_jobtitle)\
            VALUES ('%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') " % ( remove_html( clear_text(a) ), 'NOW()', 
            mysql_date_format( remove_html(b), 'kaskus_post_date'), thread_id, page_id, 
            remove_html(c), 
            encode_post(d), 
            clear_text(e), 
            remove_html( clear_text(f) ), 
            remove_html(g), 
            mysql_date_format( remove_html( clear_text(h) ), 'kaskus_join_date'),
            remove_html( clear_text(i) ) )

            try:
                cursor.execute(sql_insert)
                counter += 1
                sys.stdout.write(str(counter)+' ')
            except:
                print "key duplicate"
                print sql_insert                
        '''
        END SAVE DATA
        '''
                    
        curr_page = response.url
        next_url_1 = hxs.select('//div[@class="pagenav"]/table/tr/td[@class="alt1"][last()]/a/@href').extract()
        try:
            next_url_1 = hxs.select('//div[@class="pagination"]/ul/ul/li[last()-1]/a/@href').extract()
            next_url = base_url + next_url_1[0]
#            print next_url
#            print response.url
            
            yield Request(next_url, callback = self.parseNextUrl)
        except:
            pass 
            
        db_base.conn.commit()
