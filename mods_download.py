from __future__ import print_function
import requests
import os
import datetime
import begin
import collections
from tqdm import tqdm

DEFAULT_SOLR_URL = "https://repository.library.brown.edu/api/search/"
MODS_SERVICE_URL = 'https://repository.library.brown.edu/services/getMods/'
DEFAULT_QUERY = "NOTHING"
DEFAULT_BASE_DIR = "./downloaded"
ROWS = 500


def download_file(url, filename, base_dir=DEFAULT_BASE_DIR):
    r2 = requests.get(url)
    if r2.ok:
        tempfilename = os.path.join(base_dir, filename)
        with open(tempfilename, 'wb') as f:
            f.write(r2.content)


class Mods_Doc(collections.namedtuple("Doc", ['pid'])):
    @property
    def filename(self):
        return self.pid.replace(":", "") + ".mods.xml"

    @property
    def url(self):
        return self.uri

    @property
    def uri(self):
        return MODS_SERVICE_URL+'{}/'.format(self.pid)


def get_solr_docs(query=DEFAULT_QUERY, start=0, solr_url=DEFAULT_SOLR_URL):
    r = requests.get(
        solr_url,
        params={
            'q': query,
            'rows': ROWS,
            'fl': 'pid',
            'wt': 'json',
            'start': start,
        })
    docs = []
    if r.ok:
        numFound = r.json()['response']['numFound']
        docs = r.json()['response']['docs']
        if numFound - start > ROWS:
            docs.extend(
                get_solr_docs(
                    query=query,
                    start=start + ROWS,
                    solr_url=solr_url
                )
            )
    return docs

def setup_storage(base_dir):
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    storage_directory = os.path.join(base_dir, timestamp)
    if not os.path.exists(storage_directory):
        os.makedirs(storage_directory)
    return storage_directory


@begin.start(auto_convert=True)
def main(
        query=DEFAULT_QUERY,
        base_dir=DEFAULT_BASE_DIR):
    storage_directory = setup_storage(base_dir)
    docs = get_solr_docs(query)
    for d in tqdm(docs):
        doc = Mods_Doc(**d)
        download_file(doc.url, doc.filename, storage_directory)
