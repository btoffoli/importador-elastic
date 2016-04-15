from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()

doc = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}
res = es.index(index="meu-teste", doc_type='tweet', id=1, body=doc)
print(res['created'])

res = es.get(index="meu-teste", doc_type='tweet', id=1)
print(res['_source'])

es.indices.refresh(index="meu-teste")

res = es.search(index="meu-teste", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])