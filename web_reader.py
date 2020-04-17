from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import random
import time
from collections import Counter
from numpy.random import choice
import numpy as np
from warnings import warn
from glob import glob

#TODO: Compare to https://en.wikipedia.org/wiki/Inverse_distribution
def count_pmf(counts, inverse=False):
    '''
    This function creates a probability distrubtion
    with the property that the probability of an
    event is a function of weights associated with
    other events rather than its own.

    PARAMETERS:
        counts (Counter or Dict)

    RETURNS:
        event (list): The name of each event in the event space.
        prob (list): The probability of each event in the event space.
    '''
    d = {}
    if inverse:
        for key in counts:
            d[key] = sum([counts[k] for k in counts.keys() if k != key])
    else:
        for key in counts:
            d[key] = sum([counts[k] for k in counts.keys() if k == key])
    total = sum(d.values())
    event, prob = [], []
    for k, v in d.items():
        event.append(k)
        prob.append(v / total)
    return event, prob

class Page:
    '''
    This class definition is for treating website URLs
    as objects that can be easily handled for requests,
    parsing HTML, and getting hyperlinks.
    '''

    def __init__(self, url):
        '''
        The initilization method only requires
        the URL, which can be used as a unique ID
        among other Page objects.

        PARAMETERS:
            url (str)

        RETURNS:
            None
        '''
        self.url = url

    def get_requests(self):
        '''
        Stores a request object.

        PARAMETERS:
            None

        RETURNS:
            None
        '''
        self.requests = requests.get(self.url)

    def get_soup(self):
        '''
        Uses BeautifulSoup to parse the HTML of a website,
        assuming lxml for structure. This creates a soup
        object as a property of the Page object. Using
        this method requires that a request object already
        exists in the same Page object.

        PARAMETERS:
            None

        RETURNS:
            None
        '''
        self.soup = BeautifulSoup(self.requests.text, 'lxml')

    def get_bad_strs(self):
        '''
        Stores a list of strings which are used for
        filtering out undesired URLS. The 'bad strs' list
        comes from a local file.

        PARAMETERS:
            None

        RETURNS:
            None
        '''
        self.bad_strs = []
        with open('bad_strs.txt', 'r') as f:
            for line in f:
                self.bad_strs.append(line.replace('\n', ''))

    def get_links(self):
        '''
        Extracts all the hyperlinks from parsed HTML
        using the BeautifulSoup Soup object method "findAll".
        Using this method requires that a soup object already
        exists in the same Page object.
        
        PARAMETERS:
            None

        RETURNS:
            None
        '''
        self.links = []
        for link in self.soup.findAll('a', href=True):
            if link.get('href') and ('http' in link.get('href')):
                if not [bs for bs in self.bad_strs if bs in link.get('href')]:
                    self.links.append(link.get('href'))
            elif link.get('href') and ('/wiki/' in link.get('href')) and ('wikipedia.org' not in link.get('href')):
                if not [bs for bs in self.bad_strs if bs in link.get('href')]:
                    self.links.append('https://en.wikipedia.org' + link.get('href'))
        self.links = list(set(self.links))

    def easy_links(self):
        '''
        Once a page object is initialized, run this method
        to complete the whole process of making requests,
        initializing soup object, and extracting a list
        of hyperlinks from the soup object. This method
        is simply a wrapper to easily call the other methods
        in one command.

        PARAMETERS:
            None

        RETURNS:
            None
        '''
        self.get_requests()
        self.get_soup()
        self.get_bad_strs()
        self.get_links()

class Table:
    '''
    Defines an object to load a CSV table.
    The primary function of this class is for use within
    the DBManger object.
    '''

    def __init__(self, file):
        self.file = file
        self.table = pd.read_csv(file)

    def reload_table(self):
        self.table = pd.read_csv(file)

    def save_table(self):
        self.table.to_csv(self.file, index=False)

    def uniquify_table(self):
        self.table = self.table.drop_duplicates()

class DataBase:

    def __init__(self, read_file, relation_file):
        self.read_table = None
        self.relation_table = None
        try:
            self.read_table = Table(read_file)
        except Exception as e:
            print(read_file, e)
            warn('The read table {} is missing!'.format(read_file))
        try:
            self.relation_table = Table(relation_file)
        except Exception as e:
            print(relation_file, e)
            warn('The relation table {} is missing!'.format(relation_file))

    def reload_dbs(self):
        self.read_table.reload_table()
        self.relation_table.reload_table()

    def save_dbs(self):
        self.read_table.save_table()
        self.relation_table.save_table()

    def get_relation_dict(self):
        self.relation_dict = {}
        url_queries = list(self.read_table.table['URL'])
        for i, url in enumerate(url_queries):
            try:
                page = Page(url)
                page.easy_links()
                for link in page.links:
                    print(time.ctime(), (i+1) / len(url_queries) * 100, url, link)
                    self.relation_dict[url] = link
            except Exception as e:
                print(time.ctime(), url, e)
            time.sleep(3)

    def reldict_to_table(self):
        self.relation_table.table = pd.DataFrame({'Parent':list(self.relation_dict.keys()),
                                                  'Child':list(self.relation_dict.values())})

    def add_link(self, url):
        if url not in set(self.read_table.table['URL']):
            self.read_table.table = self.read_table.table.append(pd.DataFrame({'URL':[url],
                                                                               'Read':[1]}))
        else:
            raise Exception('{} is already in read table.'.format(url))

    def mark_as_read(self, url):
        if url in set(self.read_table.table['URL']):
            index = self.read_table.table[self.read_table.table['URL'] == url]['Read'].index
            self.read_table.table.at[index, 'Read'] = 1
        else:
            raise Exception('{} is not in read table. Use self.add_link method to add it.'.format(url))

    def query_read_table_URLs(self, query):
        return self.read_table.table[self.read_table.table['URL'].str.contains(query)]

    def core_links(self):
        core_link_set = set()
        for link in set(self.read_table.table['URL']):
            core_link_set.update({link.split('/')[2]})
        return core_link_set

    # TODO: Change or delete this method
    def envelope_site(self, url, limit_str):
        site_links = set()
        site_q = [url]
        while site_q:
            try:
                page = Page(site_q[-1])
                page.easy_links()
                site_links.update({site_q[-1]})
                site_q.pop()
                for link in (set(page.links) - site_links) - set(site_q):
                    if limit_str in link:
                        print(len(site_links), len(site_q), link)
                        site_q.append(link)
                    else:
                        continue
            except Exception as e:
                print(site_q[-1], e)
                site_q.pop()
            time.sleep(3)
        return site_links

    def recommend_random(self):
        '''
        Randomly selects and returns an unread page from the read table.

        PARAMETERS:
            None

        RETURNS:
            (str)
        '''
        return choice(list(set(self.relation_table.table['Child'])))

    def connectivity_weights(self):
        '''
        This method calculates weights for each node that are a
        polynomial function of the number of other nodes pointing to
        the node in question, and away from the node in question. A query
        string can be used to select a subset of the graph.

        PARAMETERS:
            query (str): Only URLs that contain this string will be considered.
        '''
        read_links = set(self.read_table.table['URL'])
        search_set = set(self.relation_table.table['Child']) - read_links
        if search_set:
            counts = Counter()
            for link in search_set:
                try:
                    psum = sum(self.relation_table.table['Parent'].str.contains(link))
                    csum = sum(self.relation_table.table['Child'].str.contains(link))
                    pc_prod = (1 + psum) * (1 + csum)
                    counts.update({link:pc_prod})
                except Exception as e:
                    print(link, e)
        else:
            warn('''The search set was empty. This may be due to all matching targets having been read.''')
            return None
        return counts

    def recommend_max_centrality(self, top_n=1):
        counts = self.connectivity_weights()
        return counts.most_common()[:top_n]

    def recommend_pmf_centrality(self):
        counts = self.connectivity_weights()
        pmf = count_pmf(counts)
        rand_link = choice(pmf[0], p=pmf[1])
        return rand_link, counts[rand_link]

if __name__ == '__main__':
    DB = DataBase('all_read.csv', 'all_relations.csv')
##    DB.get_relation_dict()
##    DB.reldict_to_table()
##    DB.save_dbs()
    print('Max:', DB.recommend_max_centrality())
    print('PMF:', DB.recommend_pmf_centrality())
    print('Random:', DB.recommend_random())
