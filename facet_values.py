import requests
import csv
import sys
import begin

DEFAULT_SOLR_URL = "https://repository.library.brown.edu/api/search/"
DEFAULT_FIELD = "keyword"
DEFAULT_QUERY = "*"


def write_out_results(response, field):
    kwds = response.json()['facet_counts']['facet_fields'][field]
    kwzip = zip(kwds[0::2], kwds[1::2])
    mycsv = csv.writer(sys.stdout)
    for k in kwzip:
        row = [k[0], k[1]]
        mycsv.writerows([row])


@begin.start(auto_convert=True)
def main(field=DEFAULT_FIELD, query=DEFAULT_QUERY, solr_url=DEFAULT_SOLR_URL):
    r = requests.get(
        solr_url,
        params={
            'q': query,
            "facet": 'true',
            "facet.field": field,
            'facet.mincount': 1,
            'rows': 0,
            'facet.limit': -1,
            'wt': 'json',
        })
    if r.ok:
        write_out_results(r, field)
    else:
        print("Error in results")
