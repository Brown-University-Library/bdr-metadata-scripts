import begin
import collections
import datetime
import requests
import os


from pathos.pools import ProcessPool as Pool
from functools import partial
from tqdm import tqdm

DEFAULT_SOLR_URL = "https://repository.library.brown.edu/api/search/"
MODS_URL_PATTERN = 'https://repository.library.brown.edu/storage/{pid}/MODS/'
DEFAULT_QUERY = "NOTHING"
DEFAULT_BASE_DIR = "./downloaded"
DEFAULT_PROCESSES = 4
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
        return MODS_URL_PATTERN.format(pid=self.pid)

    def save(self, save_dir=DEFAULT_BASE_DIR):
        download_file(self.url, self.filename, base_dir=save_dir)


class DocSaver:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._save_dir=self.setup_storage()

    @property
    def save_dir(self):
        if not self._save_dir:
            self._save_dir = self.setup_storage()
        return self._save_dir

    def setup_storage(self):
        base_dir = self.base_dir
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        storage_directory = os.path.join(base_dir, timestamp)
        if not os.path.exists(storage_directory):
            os.makedirs(storage_directory)
        return storage_directory

    def save(self, doc):
        download_file(doc.url, doc.filename, self.save_dir)


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



def download_doc(doc, save_dir):
    doc.save(save_dir)

@begin.start(auto_convert=True)
def main(
        query=DEFAULT_QUERY,
        number_of_processes=DEFAULT_PROCESSES,
        base_dir=DEFAULT_BASE_DIR):
    docs = get_solr_docs(query)
    saver = DocSaver(base_dir)
    pool = Pool(number_of_processes)
    mdocs = (Mods_Doc(**d) for d in docs)
    max_= len(docs)
    with tqdm(total=max_) as pbar:
        for i, _ in tqdm(enumerate(pool.imap(saver.save, mdocs))):
            pbar.update()
    pool.close()
    pool.join()
