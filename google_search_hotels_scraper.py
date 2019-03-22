import os
import sys
import socket
import urllib
import psycopg2
import random
import time
import socks

from pprint import pprint
from sockshandler import SocksiPyHandler
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen
from lxml import html
from lxml.html import fromstring

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys

from user_agents import words, words1, names, numbers, numbers1
from chrome_useragents import chrome

# timeout in seconds
timeout = 80
socket.setdefaulttimeout(timeout)

# ua = UserAgent()
headers = {}
headers['User-Agent'] = random.choice(chrome)  # ua.chrome
DBC = "dbname='deutschland_hotels' user='postgres' host='localhost' password='1122' port='5432'"

proxies = [
    ("138.185.166.171", 9999),
]

# =======================================================================


class GoogleSearchHotels():
    """
    This Scraper scrapes hotel name, phone, address, website, direction, \
    description, rating, reviews_count, reviews_rating, reviews_link, review, \
    summary_review, mapped_urls from Google search. Makes a list of queries \
    (need a list of towns, places in country you want to scrape) to be able \
    to make a Google search request. Spyder searches through socks5 proxies. \
    Collects data and stores it to Postgres.
    """
    def __init__(self, *args, **kwargs):
        self.gecko_path = os.path.abspath(os.path.curdir) + '/geckodriver'
        self.profile = webdriver.FirefoxProfile()
        self.profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', False)
        self.profile.set_preference("media.peerconnection.enabled", False)
        self.profile.set_preference('javascript.enabled', False)
        self.driver = webdriver.Firefox(firefox_profile=self.profile, executable_path=self.gecko_path)
        self.driver.implicitly_wait(15)
        self.conn = psycopg2.connect(DBC)
        self.cur = self.conn.cursor()
        self.conn.autocommit = True
        super(GoogleSearchHotels, self).__init__(*args, **kwargs)

    def create_table_hotel(self):
        """Create table hotel"""
        self.cur.execute("DROP TABLE IF EXISTS hotel")
        self.cur.execute(
            "CREATE TABLE hotel( \
                hid uuid DEFAULT uuid_generate_v4 (), \
                name TEXT UNIQUE, \
                phone VARCHAR(35), \
                address TEXT, \
                adds BOOLEAN, \
                website TEXT, \
                direction TEXT, \
                description TEXT, \
                rating VARCHAR(25), \
                reviews_count VARCHAR(25), \
                reviews_rating VARCHAR(25), \
                reviews_link TEXT, \
                review TEXT, \
                PRIMARY KEY(hid));")
        print('hotel table created')

    def create_table_facilities(self):
        """Create table facilities"""
        self.cur.execute("DROP TABLE IF EXISTS facilities")
        self.cur.execute(
            "CREATE TABLE facilities( \
                fid uuid DEFAULT uuid_generate_v4 (), \
                title TEXT UNIQUE, \
                PRIMARY KEY(fid));")
        print('facilities table created')

    def create_table_hotel_facilities(self):
        """Create table hotel_facilities"""
        self.cur.execute("DROP TABLE IF EXISTS hotel_facilities")
        self.cur.execute(
            "CREATE TABLE hotel_facilities( \
                hotel_id uuid, \
                facility_id uuid, \
                PRIMARY KEY (hotel_id, facility_id), \
                FOREIGN KEY (hotel_id) REFERENCES hotel (hid) ON DELETE CASCADE, \
                FOREIGN KEY (facility_id) REFERENCES facilities (fid) ON DELETE CASCADE);")
        print('facilities table created')

    def create_table_summary_review(self):
        """Create table summary_review"""
        self.cur.execute("DROP TABLE IF EXISTS summary_review")
        self.cur.execute(
            "CREATE TABLE summary_review( \
                srid uuid DEFAULT uuid_generate_v4 (), \
                category VARCHAR(35), \
                rating  VARCHAR(25), \
                description TEXT, \
                PRIMARY KEY(srid));")
        print('summary_review table created')

    def create_table_hotel_summary_review(self):
        """Create table hotel_summary_review"""
        self.cur.execute("DROP TABLE IF EXISTS hotel_summary_review")
        self.cur.execute(
            "CREATE TABLE hotel_summary_review( \
                hotel_id uuid, \
                sreview_id uuid, \
                PRIMARY KEY (hotel_id, sreview_id), \
                FOREIGN KEY (hotel_id) REFERENCES hotel (hid) ON DELETE CASCADE, \
                FOREIGN KEY (sreview_id) REFERENCES summary_review (srid) ON DELETE CASCADE);")
        print('hotel_summary_review table created')

    def create_table_mapped_urls(self):
        """Create table mapped_urls"""
        self.cur.execute("DROP TABLE IF EXISTS mapped_urls")
        self.cur.execute(
            "CREATE TABLE mapped_urls( \
                uid uuid DEFAULT uuid_generate_v4 (), \
                url TEXT, \
                PRIMARY KEY(uid));")
        print('mapped_urls table created')

    def create_table_hotel_mapped_urls(self):
        """Create table hotel_mapped_urls"""
        self.cur.execute("DROP TABLE IF EXISTS hotel_mapped_urls")
        self.cur.execute(
            "CREATE TABLE hotel_mapped_urls( \
                hotel_id uuid, \
                url_id uuid, \
                PRIMARY KEY (hotel_id, url_id), \
                FOREIGN KEY (hotel_id) REFERENCES hotel (hid) ON DELETE CASCADE, \
                FOREIGN KEY (url_id) REFERENCES mapped_urls (uid) ON DELETE CASCADE);")
        print('hotel_mapped_urls table created')

    def get_proxy(self):
        """Get random proxy with SocksiPyHandler"""
        proxy = random.choice(proxies)
        opener = urllib.request.build_opener(SocksiPyHandler(socks.SOCKS5, "{}".format(proxy[0]), proxy[1]))
        return opener

    def get_ip(self):
        """Get cfurrent IP"""
        url = 'http://httpbin.org/ip'
        response = urlopen(url).read().decode("utf8")
        print(response)
        return response

    def query_list(self):
        """Query list for search queries"""
        q_list = ["Binnenalster", "Alster Lakes",
                  "Luneburg Heath", "Alster River", "Neuwerk Island", "Norderelbe", "Cremon", "Messe Bremen",
                  "Freimarkt", "Bremen-Arena", "Bremer Rathaus", "Bremen Cathedral", "Zion's Church",
                  "Kunsthalle Bremen", "Town Musicians of Bremen", "Bremer Roland", "Schnoor Historic Old Town",
                  "Universum Science Center", "Rhododendron-Park Bremen", "Bottcherstrasse", "Bremen Roland",
                  "Weserstadion", "Beck's Brewery", "Bremen Central Railway Station", "Bremen University of Applied Sciences",
                  "University of the Arts Bremen", "Jacobs University Bremen", "University of Bremen", "Bremerhaven"]
        print("Q LIST: {}".format(q_list))
        print("List size (q_list) = {}".format(sys.getsizeof(q_list)))
        return q_list

    def make_string_query_list(self, q_list):
        """Make string query list"""
        str_query = []
        for qu in q_list:
            query = 'hotels in {} Deutschland'.format(qu)
            str_query.append(query)
        print("List size (str_query) = {}".format(sys.getsizeof(q_list)))
        return str_query

    def making_google_query(self, str_query):
        """Making google query"""
        google_query = []
        for qu in str_query:
            q = qu.strip().replace("\xc8", "%C8").replace("\xe0", "%E0").replace("\xdc", "%DC").replace(
                "\xf3", "%F3").replace("\xdf", "%DF").replace("\xd6", "%D6").replace("\xe8", "%E8").replace(
                "\u2122", "%u2122").replace("\xf4", "%F4").replace("\xc4", "%C4").replace("\xe2", "%E2").replace(
                "\xe4", "%E4").replace("\xfc", "%FC").replace("\xe9", "%E9").replace("\xf6", "%F6").replace(" ", "%20").replace(
                "&", "%26").replace("'", "%27").replace("*", "%2A").replace("|", "%7C").replace("\'n", "%5C%27n").replace(
                "\'", "%5C%27").replace("/", "%2F").replace(" ", "%2B")
            google_query.append(q)
        print("List size (google_query) = {}".format(sys.getsizeof(q_list)))
        return google_query

    def google_request(self, q):
        """Making google request"""
        print("Current query: {}".format(q))
        self.driver.get("https://www.google.com/search?hl=en&q={}".format(q))

    def get_amount(self):
        """Get amount of pages to go"""
        amount_obj = self.driver.find_element_by_xpath('//div[@class="DLOTif"]/span/span').text
        amount_str = amount_obj.replace("View ", "").replace(" hotels", "").replace(",", "")
        amount = int(amount_str)
        count_pages = round(amount / 20)
        return count_pages

    def click_in_google(self):
        """Click button"""
        try:
            button = self.driver.find_element_by_class_name("cMjHbjVt9AZ__button")
            button.click()
            time.sleep(3)
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def click_finish_button(self):
        """Click finish button"""
        try:
            button_finish = self.driver.find_element_by_tag_name("g-raised-button")
            button_finish.click()
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def collect_hotel_objs(self):
        """Collect hotel objs on current page"""
        try:
            hotels_obj = self.driver.find_elements_by_xpath('//div[@class="VkpGBb"]/a')
            print("Collecting hotel objs: {}".format(len(hotels_obj)))
            return hotels_obj
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def collect_hotel_objs_names(self):
        """Collect hotel name objs on current page"""
        hotels_obj_names = self.driver.find_elements_by_xpath('//div[@class="dbg0pd"]/div')
        name = [n.text for n in hotels_obj_names]
        print("Collecting hotel objs names: {}".format(len(name)))
        return name

    def check_if_hotel_name_is_in_db(self, name):
        """Check if hotel name is already in_db"""
        select_hid = "SELECT hid FROM hotel WHERE name = '{}';".format(str(name).replace("'", "\'"))
        self.cur.execute(select_hid)
        hid = self.cur.fetchone()
        return hid

    def select_all_hotel_names_from_db(self):
        """Select_all_hotel_names_from_db"""
        self.cur.execute("SELECT name FROM hotel;")
        names = self.cur.fetchall()
        names_db = {str(name).replace("('", "").replace("',)", "") for name in names}
        return names_db

    def hotel_obj_click(self, obj):
        """Click on hotel obj to get to hotel data"""
        obj.send_keys(Keys.SPACE)
        try:
            obj.click()
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_adds(self):
        """Get hotel adds data"""
        try:
            adds = self.driver.find_element_by_class_name('B4MzEf').text
            if not adds:
                adds = False
            else:
                adds = True
            print("GOT ADDS: {}".format(adds))
            return adds
        except NoSuchElementException as err:
            print("{}".format(err))

    def get_hotel_name(self):
        """Get hotel name data"""
        try:
            name = self.driver.find_element_by_xpath('//div[@class="SPZz6b"]/div/span').text
            print("Name: {}".format(name or None))
            return name
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_website(self):
        """Get hotel website data"""
        try:
            website = self.driver.find_element_by_xpath(
                '//div[@class="QqG1Sd"]/a[@class="CL9Uqc ab_button"]').get_attribute("href")
            print("Website: {}".format(website or None))
            return website
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_direction(self):
        """Get hotel direction data"""
        try:
            direction = self.driver.find_element_by_xpath(
                '//div[@class="QqG1Sd"]/a[@class="CL9Uqc ab_button"]').get_attribute("href")
            print("Direction: {}".format(direction or None))
            return direction
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_rating(self):
        """Get hotel rating data"""
        try:
            rating = self.driver.find_element_by_xpath('//span[@class="YhemCb"]').text
            print("Hotel_rating: {}".format(rating or None))
            return rating
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_reviews_count(self):
        """Get hotel reviews_count data"""
        try:
            reviews_count = self.driver.find_element_by_xpath('//div/span[@class="fl"]/span/a/span').text
            print("Reviews_count: {}".format(reviews_count or None))
            return reviews_count
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_reviews_rating(self):
        """Get hotel reviews_rating data"""
        try:
            reviews_rating = self.driver.find_element_by_xpath('//div[@class="Ob2kfd"]/div/span').text
            print("Reviews_rating: {}".format(reviews_rating or None))
            return reviews_rating
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_reviews_link(self):
        """Get hotel reviews_link data"""
        try:
            reviews_link = self.driver.find_element_by_xpath(
                '//div/span[@class="fl"]/span/a').get_attribute("data-fid")
            reviews_full_link = 'https://www.google.com/search?hl=en&q={}'.format(
                q) + "#lrd=" + str(reviews_link).replace("['", "").replace("']", "") + ",1,,,"
            print("Reviews_full_link: {}".format(reviews_full_link or None))
            return reviews_full_link
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_address(self):
        """Get hotel address data"""
        try:
            address = self.driver.find_element_by_xpath('//div[@class="Z1hOCe"]/div/span[@class="LrzXr"]').text
            print("Address: {}".format(address or None))
            return address
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_phone(self):
        """Get hotel phone data"""
        try:
            phone = self.driver.find_element_by_xpath('//div[@class="Z1hOCe"]/div/span/span/span').text
            print("Phone: {}".format(phone or None))
            return phone
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def click_to_open_description(self):
        """Click to open description"""
        try:
            self.driver.find_element_by_xpath('//span[@class="CeVKi"]/span').click()
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_description(self):
        """Get hotel description data"""
        try:
            description = self.driver.find_element_by_xpath('//div[@id="rUnked"]/span/span').text
            print("Description: {}".format(description or None))
            return description
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_review(self):
        """Get hotel review data"""
        try:
            review_objs = self.driver.find_elements_by_xpath('//div[@class="RfWLue a1VOGd nQA3jb"]')
            objs = [obj.text for obj in review_objs]
            str_obj = ' '.join(objs)
            review = str_obj.replace('" "', '. ').replace('"', '').replace("....", "...")
            print("Review: {}".format(review))
            return review
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def click_to_open_facilities(self):
        """Click to open facilities"""
        try:
            self.driver.find_element_by_xpath('//div[@class="EF00Ab ZiqWQe lhamb"]').click()
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_facilities(self):
        """Get hotel facilities data"""
        try:
            services_obj = self.driver.find_elements_by_xpath(
                '//div[@class="oxkUqd dcgNrc"]/span/span/span[@class="THkfd"]')
            services1 = [obj.text for obj in services_obj]
            services_obj1 = self.driver.find_elements_by_xpath(
                '//div[@class="olHr7b dcgNrc"]/span/span/span[@class="THkfd"]')
            services2 = [obj.text for obj in services_obj1]
            facilities = services1 + services2
            print("Facilities: {}".format(facilities or None))
            return facilities
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_reviews_query(self, name, address):
        """Get hotel reviews_query"""
        r_query = 'hotel+"' + name + '"+' + address
        reviews_query = r_query.strip().replace("\xc8", "%C8").replace(
            "\xe0", "%E0").replace("\xdc", "%DC").replace("\xf3", "%F3").replace("\xdf", "%DF").replace(
            "\xd6", "%D6").replace("\xe8", "%E8").replace("\u2122", "%u2122").replace("\xf4", "%F4").replace(
            "\xc4", "%C4").replace("\xe2", "%E2").replace("\xe4", "%E4").replace("\xfc", "%FC").replace(
            "\xe9", "%E9").replace("\xf6", "%F6").replace(" ", "%20").replace("&", "%26").replace(
            "'", "%27").replace("*", "%2A").replace("|", "%7C").replace("\'n", "%5C%27n").replace(
            "\'", "%5C%27").replace("/", "%2F").replace(" ", "%2B")
        return reviews_query

    def make_request_to_get_summary_reviews(self, reviews_query):
        """Make request to get summary reviews"""
        print("Reviews Query: {}".format(reviews_query))
        req = Request('https://www.google.com/search?hl=en&q={}'.format(reviews_query), headers=headers)
        webpage = urlopen(req).read()
        w = webpage.decode('utf8')
        html_tree = html.fromstring(w)
        return html_tree

    def get_sidebar_content_block(self, html_tree):
        """Get sidebar content block"""
        try:
            block = html_tree.xpath('//div[@id="rhs_block"]/h1[@class="bNg8Rb"]/text()')
            return block
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_summary_review_names(self, html_tree):
        """Get hotel summary review names"""
        try:
            summary_review_names = html_tree.xpath(
                '//div[@class="NsRfAb XMibRe"]/div[@class="jlBtR"]/span[@class="zSN9Zd"]/text()')
            return summary_review_names
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_summary_review_ratings(self, html_tree):
        """Get hotel summary review ratings"""
        try:
            summary_review_ratings = html_tree.xpath(
                '//div[@class="NsRfAb XMibRe"]/div[@class="jlBtR"]/span[@class="Y0jGr"]/span[@class="rtng"]/text()')
            return summary_review_ratings
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_summary_review_texts(self, html_tree):
        """Get hotel summary review texts"""
        try:
            row_texts = html_tree.xpath('//div[@class="NsRfAb XMibRe"]/div[@class="jlBtR"]/div/span/text()')
            row = '__'.join(row_texts)
            rrow = row.replace("__·__", " · ")
            summary_review_texts = rrow.split("__")
            return summary_review_texts
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def click_some_button(self):
        """Click on a button"""
        try:
            self.driver.find_element_by_xpath('//div[@class="oifQgc"]').click()
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_links_to_map(self):
        """Get hotel links to map"""
        try:
            a_links_objs = self.driver.find_elements_by_xpath(
                '//div[@class="B4MzEf"]/a[@class="Ba8ysd a-no-hover-decoration"]')
            a_links = [obj.get_attribute("href") for obj in a_links_objs]
            print('AD URLS COUNT: {}'.format(len(a_links)))
            return a_links
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def get_hotel_mapped_urls(self, links_to_mapp):
        """Get hotel mapped urls"""
        try:
            mapped_urls = []
            for a_link in links_to_mapp:
                header = {}
                agent = ['{}:{}:v{}.{} (by /u/{})'.format(
                    random.choice(words), random.choice(words1),
                    random.choice(numbers), random.choice(numbers1), random.choice(names))]
                header['User-Agent'] = random.choice([agent])
                print("<-------=======------->")
                header = {'User-Agent': random.choice(header['User-Agent'])}
                print('Agent-Smith: {}'.format(header))
                req_intermediate_mapped_link = Request(a_link, headers=header)
                mapped = urlopen(req_intermediate_mapped_link).geturl()
                mapped_urls.append(mapped)
                pprint('URL: {}'.format(mapped))
            return mapped_urls
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def insert_hotel_to_db(self, *args):
        """Insert hotel data to Postgres"""
        self.cur.execute("INSERT INTO hotel ( \
            adds, name, phone, address, website, direction, description, rating, \
            reviews_count, reviews_rating, reviews_link, review) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (
            adds, name, phone, address, website, direction, description, rating,
            reviews_count, reviews_rating, reviews_link, review))

    def select_fid(self, facility):
        """SELECT fid FROM facilities"""
        select_fid = "SELECT fid FROM facilities WHERE title = '{}';".format(facility)
        self.cur.execute(select_fid)
        fid = self.cur.fetchone()
        return fid

    def insert_fid(self, facility):
        """INSERT INTO facilities (title)"""
        insert_fid = "INSERT INTO facilities (title) VALUES (%s);"
        self.cur.execute(insert_fid, (facility,))

    def insert_to_hotel_facilities_table(self, hid, fid):
        """Insert to hotel facilities table"""
        insert = "INSERT INTO hotel_facilities (hotel_id, facility_id) VALUES (%s, %s);"
        self.cur.execute(insert, (hid, fid))

    def select_srid(self, review):
        """SELECT srid FROM summary_review"""
        select_srid = "SELECT srid FROM summary_review WHERE description = '{}';".format(review[2])
        self.cur.execute(select_srid)
        srid = self.cur.fetchone()
        return srid

    def insert_srid(self, review):
        """INSERT INTO summary_review (category, rating, description)"""
        insert = "INSERT INTO summary_review (category, rating, description) VALUES (%s, %s, %s);"
        self.cur.execute(insert, (review[0], review[1], review[2]))

    def insert_to_hotel_summary_review_table(self, hid, rid):
        """INSERT INTO hotel_summary_review (hotel_id, sreview_id)"""
        insert = "INSERT INTO hotel_summary_review (hotel_id, sreview_id) VALUES (%s, %s);"
        self.cur.execute(insert, (hid, rid))

    def select_uid(self, url):
        """SELECT uid FROM mapped_urls"""
        select_uid = "SELECT uid FROM mapped_urls WHERE url = '{}';".format(url)
        self.cur.execute(select_uid)
        uid = self.cur.fetchone()
        return uid

    def insert_uid(self, url):
        """INSERT INTO mapped_urls (url)"""
        insert_uid = "INSERT INTO mapped_urls (url) VALUES (%s);"
        self.cur.execute(insert_uid, (url,))

    def insert_to_hotel_mapped_urls_table(self, hid, uid):
        """INSERT INTO hotel_mapped_urls (hotel_id, url_id)"""
        insert_to_hotel_mapped_urls = "INSERT INTO hotel_mapped_urls (hotel_id, url_id) VALUES (%s, %s);"
        self.cur.execute(insert_to_hotel_mapped_urls, (hid, uid))

    def next_page_url(self):
        """Get next page url"""
        try:
            next_a = self.driver.find_element_by_xpath('//a[@id="pnnext"]').get_attribute("href")
            print("Clicking on next page button.")
            self.driver.get(next_a)
            time.sleep(3)
        except NoSuchElementException as err:
            print("{}".format(err))
            pass

    def next_page(self):
        """Next page url obj"""
        next_a = self.driver.find_element_by_xpath('//a[@id="pnnext"]').get_attribute("href")
        return next_a


if __name__ == "__main__":
    g = GoogleSearchHotels()
    # g.create_table_hotel()
    # g.create_table_facilities()
    # g.create_table_hotel_facilities()
    # g.create_table_summary_review()
    # g.create_table_hotel_summary_review()
    # g.create_table_mapped_urls()
    # g.create_table_hotel_mapped_urls()
    # opener = g.get_proxy()
    g.get_ip()
    q_list = g.query_list()
    str_query = g.make_string_query_list(q_list)
    google_query_list = g.making_google_query(str_query)
    for q in google_query_list:
        t0 = time.time()
        g.google_request(q)
        amount = g.get_amount()
        print("AMOUNT of hotels to scrape: {}".format(amount))
        g.click_in_google()
        g.click_finish_button()

        for i in range(0, int(amount)):
            try:
                h_names = g.collect_hotel_objs_names()
                h_objs = g.collect_hotel_objs()
                names_objs = list(zip(h_names, h_objs))
                for name, obj in names_objs:
                    all_db_names = g.select_all_hotel_names_from_db()
                    if name in all_db_names:
                        print("BYPASS : {}".format(name))
                        pass
                    elif name == 'elements pure FENG SHUI HOTEL Bremen':
                        pass
                    elif name == 'ATLANTIC Hotel Universum':
                        pass
                    elif name == 'Hotel Munte am Stadtwald - Ringhotel':
                        pass
                    elif name == 'AMERON Hamburg Hotel Speicherstadt':
                        pass
                    elif name == 'Grand Elysée Hamburg':
                        pass
                    elif name == 'Superbude Hotel & Hostel St. Pauli':
                        pass
                    elif name == 'Empire Riverside Hotel Hamburg':
                        pass
                    elif name == 'ibis Hotel Hamburg City':
                        pass
                    elif name == 'a&o Hostel Hamburg Reeperbahn':
                        pass
                    # elif name == 'ATLANTIC Hotel Sail City':
                    #     pass
                    elif name == 'arcona LIVING BREMEN':
                        pass
                    # elif name == 'Comfort Hotel Bremerhaven':
                    #     pass
                    # elif name == 'Hotel Buthmann im Zentrum':
                    #     pass
                    elif name == 'Best Western Hotel zur Post':
                        pass
                    # elif name == "Grothenn's Hotel":
                    #     pass
                    # elif name == "Bohn's Gästehaus":
                    #     pass
                    elif name == "Fleming's Brasserie":
                        pass
                    else:
                        g.hotel_obj_click(obj)
                        time.sleep(2)
                        name = g.get_hotel_name()
                        adds = g.get_hotel_adds()
                        if not adds:
                            adds = False
                        website = g.get_hotel_website()
                        direction = g.get_hotel_direction()
                        rating = g.get_hotel_rating()
                        reviews_count = g.get_hotel_reviews_count()
                        reviews_rating = g.get_hotel_reviews_rating()
                        reviews_link = g.get_hotel_reviews_link()
                        review = g.get_review()
                        address = g.get_hotel_address()
                        phone = g.get_hotel_phone()
                        g.click_to_open_description()
                        description = g.get_hotel_description()
                        g.click_to_open_facilities()
                        facility_objs = g.get_hotel_facilities()
                        reviews_query = g.get_hotel_reviews_query(name, address)
                        html_tree = g.make_request_to_get_summary_reviews(reviews_query)
                        block = g.get_sidebar_content_block(html_tree)
                        if not block:
                            pass

                        summary_review = list(zip(
                            g.get_summary_review_names(html_tree),
                            g.get_summary_review_ratings(html_tree),
                            g.get_summary_review_texts(html_tree)))
                        print("Summary review: {}".format(summary_review))

                        g.click_some_button()
                        links_to_mapp = g.get_hotel_links_to_map()
                        if not links_to_mapp:
                            pass
                        mapped_urls = g.get_hotel_mapped_urls(links_to_mapp)

                        hid = g.check_if_hotel_name_is_in_db(name)
                        print("HID: {}".format(hid))
                        if hid is None:
                            print("No HID, adding to hotel table")
                            g.insert_hotel_to_db(adds, name, phone, address, website, direction,
                                                 description, rating, reviews_count,
                                                 reviews_rating, reviews_link, review)
                            hid = g.check_if_hotel_name_is_in_db(name)
                            print("Hid: {}".format(hid))

                        for facility in facility_objs:
                            fid = g.select_fid(facility)
                            if fid is None:
                                print("No FID, adding to facilities table")
                                g.insert_fid(facility)
                                fid = g.select_fid(facility)
                                print("FID: {}".format(fid))
                            print("Adding to facilities table")
                            g.insert_to_hotel_facilities_table(hid, fid)
                            print("Done insert_to_hotel_facilities_table where hid : {} and fid : {}".format(hid, fid))

                        for sreview in summary_review:
                            srid = g.select_srid(sreview)
                            if srid is None:
                                print("No SRID, adding to summary_review table")
                                g.insert_srid(sreview)
                                srid = g.select_srid(sreview)
                                print("SRID: {}".format(srid))
                            print("Adding to hotel_summary_review table")
                            g.insert_to_hotel_summary_review_table(hid, srid)
                            print("Done insert_to_hotel_summary_review_table where srid : {} and srid : {}".format(hid, srid))

                        for url in mapped_urls:
                            uid = g.select_uid(url)
                            if uid is None:
                                print("No UID, adding to mapped_urls table")
                                g.insert_uid(url)
                                uid = g.select_uid(url)
                                print("UID: {}".format(uid))
                            print("Adding to hotel_mapped_urls table")
                            g.insert_to_hotel_mapped_urls_table(hid, uid)
                            print("Done insert_to_hotel_mapped_urls_table where hid : {} and fid : {}".format(hid, fid))

                g.next_page_url()
                time.sleep(3)

            except NoSuchElementException as err:
                print("{}".format(err))
            except HTTPError as e:
                print('The server couldn\'t fulfill the request.')
                pprint('Error code: {}'.format(e.code))
                if e.code == 503:
                    print("503 Reason: {}".format(e))
                    pass
                elif e.code == 502:
                    print("502 Reason: {}".format(e))
                    pass
                elif e.code == 500:
                    print("500 Reason: {}".format(e))
                    pass
                elif e.code == 404:
                    print("404 Reason: {}".format(e))
                    pass
                elif e.code == 403:
                    print("403 Reason: {}".format(e))
                    pass
                elif e.code == 400:
                    print("403 Reason: {}".format(e))
                    pass
                elif e.code == 429:
                    print("429 Reason: {}".format(e))
                    pass
                else:
                    raise
            except URLError as e:
                print('We failed to reach a server.')
                pprint('Reason: {}'.format(e.reason))
            except UnicodeEncodeError as err:
                pprint('UnicodeEncodeError code: {}'.format(err))
            except IndexError as err:
                pprint("Empty: {}".format(err))
            except KeyError as err:
                pprint('KeyError: {}'.format(err))
            except RuntimeError as err:
                pprint('RuntimeError: {}'.format(err))
            except KeyboardInterrupt as err:
                pprint('KeyboardInterrupt: {}'.format(err))
                sys.exit()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            except IOError:
                print("Connection error! (Check proxy)")
            except Exception as x:
                print('It failed :(', x.__class__.__name__)
            else:
                print("All is fine")
            finally:
                t1 = time.time()
                print('Took', t1 - t0, 'seconds')
