

"""
scrapes the given URL looking for dead resources on the same domain
"""

from time import sleep
from BeautifulSoup import BeautifulSoup as BS
from urlparse import urlparse, urljoin
from multiprocessing import Queue
import multiprocessing
from Queue import Empty
import requests
from requests import HTTPError
from collections import namedtuple
from threading import Thread
from multiprocessing import Process

version_info = (0, 0, 1)
__version__ = '.'.join(map(str, version_info))
version = __version__

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
        return HTTP_CACHE.get(url)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except (HTTPError, Exception), ex:
        print 'exception getting url content [%s]: %s' % (url,ex)
        HTTP_CACHE[url] = None
        return None
    HTTP_CACHE[url] = response.content
    return response.content

def url_to_soup(url):
    """ requests url, returns soup """
    if url in SOUP_CACHE:
        return SOUP_CACHE.get(url)
    html = url_to_content(url)
    if html is None:
        SOUP_CACHE[url] = None
        return None
    try:
        soup = BS(html)
    except Exception, ex:
        print 'exception creating soup [%s]: %s' % (url,ex)
        return None
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

class Threadify(type):
    thread_class = Thread
    def __new__(cls, name, bases, dct):
        # add thread to base classes
        print 'DCT: %s' % dct
        bases = bases + (cls.thread_class,)
        original_init = dct.get('__init__')
        if not original_init and len(bases) == 2:
            # TODO: use the python protocol for finding
            #       attrs to get init instead of using bases0
            original_init = bases[0].__init__
        # what function is the target for the thread ?
        target_str = dct.get('_thread_target')
        assert target_str, "must define run target"
        # setup new init which init's thread
        def __init__(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            cls.thread_class.__init__(self)
        dct['__init__'] = __init__
        # pull out target method
        to_run = dct.get(target_str)
        if not to_run and len(bases) == 2:
            # TODO: use protocol not bases0
            to_run = getattr(bases[0],target_str)
        # setup function which will replace old target
        # and starts the thread instead
        def call_start(self, *args, **kwargs):
            print 'starting: %s' % name
            self._run_args = args
            self._run_kwargs = kwargs
            self.start()
        # set our new start caller as the target
        dct[target_str] = call_start
        # setup a run method which just calls our
        # original target method
        def run(self):
            to_run(self, *self._run_args, **self._run_kwargs)
        # add in our run method
        dct['run'] = run
        return super(Threadify, cls).__new__(cls, name, bases, dct)
class Processify(Threadify):
    thread_class = Process

class Pool(object):
    def __init__(self, cls, args=[], kwargs={}, count=10):
        self.cls = cls
        self.count = count
        self.args = args
        self.kwargs = kwargs
        self.items = []
        self._populate()

    def _populate(self):
        root_item = self.cls(*self.args, **self.kwargs)
        self.items.append(root_item)
        # figure out which attrs are queue's
        queue_attrs = {}
        for attr in dir(root_item):
            v = getattr(root_item, attr)
            if v and v.__class__ == multiprocessing.queues.Queue:
                queue_attrs[attr] = v
            elif v and type(v) == set:
                queue_attrs[attr] = v
        for i in xrange(self.count - 1):
            item = self.cls(*self.args, **self.kwargs)
            for attr, q in queue_attrs.iteritems():
                setattr(item, attr, q)
            self.items.append(item)
        return root_item

    def __getattr__(self, attr):
        if self.items and hasattr(self.items[0],attr):
            if callable(getattr(self.items[0],attr)):
                def call_all(*args, **kwargs):
                    r = []
                    for item in self.items:
                        v = getattr(item,attr)
                        r.append(v(*args, **kwargs))
                    return r
                return call_all
            else:
                return [getattr(i,v) for i in items]
        else:
            raise AttributeError

    def start(self):
        # make one
        for item in self.items:
            item.start()
        return True

    def join(self):
        for item in self.items:
            item.join()
        return True


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

class ScraperThread(Scraper):
    __metaclass__ = Threadify
    _thread_target = 'scrape'

class ScraperProcess(Scraper):
    __metaclass__ = Processify
    _thread_target = 'scrape'

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
        for url in get_soup_images(page_url, soup):
            if url not in self.seen:
                print 'found: %s => %s' % (page_url, url)
                r = Resource(url, page_url)
                self.found_resource_queue.put(r)
                self.seen.add(url)

        for url in get_soup_links(page_url, soup):
            if url not in self.seen:
                print 'found: %s => %s' % (page_url, url)
                r = Resource(url, page_url)
                self.found_resource_queue.put(r)
                self.seen.add(url)

class ResourceFinderThread(ResourceFinder):
    __metaclass__ = Threadify
    _thread_target = 'find'

class ResourceFinderProcess(ResourceFinder):
    __metaclass__ = Processify
    _thread_target = 'find'

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

class VerifierThread(Verifier):
    __metaclass__ = Threadify
    _thread_target = 'verify'

class VerifierProcess(Verifier):
    __metaclass__ = Processify
    _thread_target = 'verify'

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

def run_threaded(root_url):
    # setup our scraper, will find all the pages
    # and the links on the site
    scrapers = Pool(ScraperProcess,(root_url,))
    root_scraper = scrapers.items[0]

    # resource finder looks at the urls and finds all the
    # resources it links to
    finders = Pool(ResourceFinderProcess,(root_url,))
    root_finder = finders.items[0]
    root_finder.page_url_queue = root_scraper.found_links_queue

    # the verifier checks the resources to make sure they exist
    verifiers = Pool(VerifierProcess, (root_url,))
    root_verifier = verifiers.items[0]
    root_verifier.unchecked_resource_queue = root_finder.found_resource_queue

    sleep(10)

    # run our scraper
    scrapers.scrape()

    # now run our finder
    finders.find()

    # and now our verifier
    verifiers.verify()

    # let them run as long as they want

    print 'joining scraper'
    scrapers.join()
    print 'joining finder'
    finders.join()
    print 'joining verifier'
    verifiers.join()

    return verifiers[0]

def run_processed(root_url, count=10):

    ### THIS HAS A PROBLEM
    # since each is in it's own proc they dont share
    # the seen set

    # setup our scraper, will find all the pages
    # and the links on the site
    scrapers = Pool(ScraperProcess,(root_url,))
    root_scraper = scrapers.items[0]

    # resource finder looks at the urls and finds all the
    # resources it links to
    finders = Pool(ResourceFinderProcess,(root_url,))
    root_finder = finders.items[0]
    root_finder.page_url_queue = root_scraper.found_links_queue

    # the verifier checks the resources to make sure they exist
    verifiers = Pool(VerifierProcess, (root_url,))
    root_verifier = verifiers.items[0]
    root_verifier.unchecked_resource_queue = root_finder.found_resource_queue

    # run our scraper
    scrapers.scrape()

    # now run our finder
    finders.find()

    # and now our verifier
    verifiers.verify()

    # let them run as long as they want

    print 'joining scraper'
    scrapers.join()
    print 'joining finder'
    finders.join()
    print 'joining verifier'
    verifiers.join()

    return verifiers[0]

if __name__ == '__main__':
    import sys
    root_url = sys.argv[1]
    print 'target: %s' % root_url
    if 'thread' in sys.argv:
        verifier = run_threaded(root_url)
    elif 'process' in sys.argv:
        verifier = run_processed(root_url)
    else:
        verifier = run(root_url)
    print 'REPORT ==============='
    bad = iterqueue(verifier.bad_resource_queue,0)
    bad = sorted(bad, key=lambda r: r[1])
    for resource_url, page_url in bad:
        print '%s :: %s' % (page_url, resource_url)
