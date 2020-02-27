# Google Search Hotel Scraper

This Scraper scrapes hotel name, phone, address, website, direction, description, rating, reviews_count, reviews_rating, reviews_link, review, summary_review, mapped_urls from Google search. Makes a list of queries (need a list of towns, places in country you want to scrape) to be able to make a Google search request. Spyder searches through socks5 proxies. Collects hotel data and stores it to PostgreSQL database.

## To install requirements and start the application:

* virtualenv -p python3.6 google_search_hotels
* cd google_search_hotels
* activate it (source bin/activate)
* git clone https://github.com/w-e-ll/google_search_hotels.git
* cd google-search-hotels
* pip install -r requirements.txt
* python google_search_hotels_scraper.py

made by: https://w-e-ll.com
