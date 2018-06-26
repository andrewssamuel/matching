import es_connection_manager as esconn
import mysql_connection_manager as sqlconn

def assemble_bulk_records():
    titles = sqlconn.query_title_matching_records()
    bulk_data = []
    for title in titles:
        try:
            release_dates=sqlconn.query_other_release_dates(title['movie_id'])
            a = dict()
            a['date'] = release_dates
            header = {"index" : 
                     {
                        "_index" : 'title_releases',
                        "_type" : 'relasedates',
                        "_id" : title['movie_id'],
                     }
                 }

            bulk_data.append(header)
            bulk_data.append(a)
        except Exception as e:
            print(e)
    return bulk_data    

def push_data_to_elastic():
    es = esconn.get_elastic_conn()
    bulk_data = assemble_bulk_records()
    es.bulk(index='title_releases', body=bulk_data)
    
   
def main():
    push_data_to_elastic()

if __name__ == '__main__':
    main()
       