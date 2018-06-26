from mysql_dbconfig import read_es_config
import elasticsearch

def get_elastic_conn():
    try:
        esconfig = read_es_config()
   
        es = elasticsearch.Elasticsearch(
            [esconfig['host']],
            http_auth=(esconfig['username'], esconfig['password']),
            port=esconfig['port'],
        )
        return es
    except Exception as ex:
        print("Error:", ex)
