

"""
scrapes the given URL looking for dead resources on the same domain
"""


from BeautifulSoup import BeautifulSoup as BS
from urlparse import urlparse, urljoin
from multiprocessing import Queue
from Queue import Empty
import requests
from requests import HTTPError
from collections import namedtuple

Resource = namedtuple('Resource',['url','ref_page'])

# lookup of http responses
HTTP_CACHE = {}
# lookup of soups
SOUP_CACHE = {}

def iterqueue(q, block_time=10):
    """ yields up items in queue """
    while True:
        try:
            v = q.get(block=bool(block_time),timeout=block_time)
        except Empty:
            break
        yield v

def url_to_content(url):
    if url in HTTP_CACHE:
        print 'HTTP CACHE'
        return HTTP_CACHE.get(url)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except (HTTPError, Exception), ex:
        print 'exception getting url content: %s' % ex
        HTTP_CACHE[url] = None
        return None
    HTTP_CACHE[url] = response.content
    return response.content

def url_to_soup(url):
    """ requests url, returns soup """
    if url in SOUP_CACHE:
        print 'SOUP CACHE'
        return SOUP_CACHE.get(url)
    html = url_to_content(url)
    if html is None:
        SOUP_CACHE[url] = None
        return None
    soup = BS(html)
    SOUP_CACHE[url] = soup
    return soup

def off_root(root_url, url):
    return urlparse(root_url).netloc == urlparse(url).netloc

def get_soup_links(url,soup):
    # now find all the links in the page
    for link in soup.findAll('a'):
        link_href = urljoin(url, link.get('href'))
        yield link_href

def get_soup_images(url,soup):
    for img in soup.findAll('img'):
        img_src = urljoin(url, img.get('src'))
        yield img_src



class Scraper(object):
    def __init__(self, root_url):

        self.root_url = root_url
        self.pages_to_spider_queue = Queue()
        self.found_links_queue = Queue()
        self.spidered_pages = set()
        self.seen = set()

        # the root url is assumed to be good
        self.pages_to_spider_queue.put(self.root_url)
        self.found_links_queue.put(self.root_url)
        self.spidered_pages.add(self.root_url)

    def scrape(self):

        # go though urls
        print 'SCRAPING'
        for url in iterqueue(self.pages_to_spider_queue):
            self._scrape(url)

    def _scrape(self, url):

        print 'scraping: %s' % url

        # request the url
        soup = url_to_soup(url)

        if not soup:
            return

        # get the href for all the links on the page
        for href in get_soup_links(url,soup):

            # if we haven't seen this href before
            if href not in self.spidered_pages:

                # add it to seen
                self.spidered_pages.add(url)

                # if the links points to another part
                # of the same site, add it to be spidered / found
                if off_root(self.root_url, href):
                    if href not in self.seen:
                        print 'adding to scrape: %s' % href
                        self.pages_to_spider_queue.put(href)
                        self.found_links_queue.put(href)
                        self.seen.add(href)


class ResourceFinder(object):
    def __init__(self, root_url):
        self.root_url = root_url
        self.page_url_queue = Queue()
        self.found_resource_queue = Queue()
        self.seen = set()

    def find(self):

        # go through each page
        print "FINDING"
        for url in iterqueue(self.page_url_queue):
            self._find(url)

    def _find(self, page_url):

        print 'finding: %s' % page_url

        # get the pages soup
        soup = url_to_soup(page_url)

        if not soup:
            return False

        # find all the images / links
        for url in get_soup_images(self.root_url, soup):
            if url not in self.seen:
                print 'found: %s => %s' % (page_url, url)
                r = Resource(url, page_url)
                self.found_resource_queue.put(r)
                self.seen.add(url)

        for url in get_soup_links(self.root_url, soup):
            if url not in self.seen:
                print 'found: %s => %s' % (page_url, url)
                r = Resource(url, page_url)
                self.found_resource_queue.put(r)
                self.seen.add(url)


class Verifier(object):
    def __init__(self, root_url):
        self.root_url = root_url
        self.unchecked_resource_queue = Queue()
        self.bad_resource_queue = Queue()
        self.good_resource_queue = Queue()
        self.seen = set()

    def verify(self):

        # go through our resources
        print 'VERIFYING'
        for url, ref_page in iterqueue(self.unchecked_resource_queue):
            if url not in self.seen:
                self._verify(url, ref_page)
                self.seen.add(url)

    def _verify(self, url, ref_page):
            # see if we can get the resource
            resource = url_to_content(url)

            # if we got the resource, it's good
            if resource and len(resource):
                print 'verified good: %s' % url
                r = Resource(url, ref_page)
                self.good_resource_queue.put(r)
            else:
                print 'verified bad: %s' % url
                r = Resource(url, ref_page)
                self.bad_resource_queue.put(r)


def run(root_url):

    # setup our scraper, will find all the pages
    # and the links on the site
    scraper = Scraper(root_url)

    # resource finder looks at the urls and finds all the
    # resources it links to
    finder = ResourceFinder(root_url)
    finder.page_url_queue = scraper.found_links_queue

    # the verifier checks the resources to make sure they exist
    verifier = Verifier(root_url)
    verifier.unchecked_resource_queue = finder.found_resource_queue

    # run our scraper
    scraper.scrape()

    # now run our finder
    finder.find()

    # and now our verifier
    verifier.verify()

    return verifier
