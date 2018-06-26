from mysql_connection_manager import query_title_matching_records,check_source_title_exists,insert_matched_title,query_other_release_dates,fetch_titles_for_fuzzy_match
import es_connection_manager as esconn
import re


def elastic_exact_query(source, magic_title_name, magic_us_release_date):
    response = esconn.get_elastic_conn().search(index='allsourcetitles', body={
        "query": {
            "bool": {
                "must": [
                    {"match": {"s_title_name.keyword": magic_title_name}},
                    {"match": {"s_release_date": magic_us_release_date}},
                    {"match": {"s_source_name": source}}
                ]
            }
        }
    })
    return response

def elastic_fuzzy_query(source, magic_title_name, magic_us_release_date):
    response = esconn.get_elastic_conn().search(index='allsourcetitles', body={
        #"min_score" : 17,
        "query": {
            "bool": {
                "must": [
                    {"match": {"s_title_name": magic_title_name}},
                    {"match": {"s_release_date": magic_us_release_date}},
                    {"match": {"s_source_name": source}}
                ]
            }
        }
    })
    return response


def find_fuzzy_match(source):
    count=0
    for title in fetch_titles_for_fuzzy_match(source.lower()+'_id'):
        count +=1
        print(count, end='\r')
        response = elastic_fuzzy_query(source,title['movie_name'], title['release_date'])
        if(response['hits']['total']==1):
            f_mattched_list = list()
            for hit in response['hits']['hits']:
                f_mattched_list.append(hit)
            if clean_title(f_mattched_list[0]['_source']['s_title_name'],title['movie_name']):
                insert_matched_title(source,3, f_mattched_list[0], title,'90')
        else:
            print('*'*100)
            print(response)
            print('*'*100) 

def find_fuzzy_match_with_date(source,title,release_date):
    response = elastic_fuzzy_query(source,title['movie_name'], release_date)
    if(response['hits']['total']==1):
        f_mattched_list = list()
        for hit in response['hits']['hits']:
            f_mattched_list.append(hit)
            clean_title_flag = clean_title(f_mattched_list[0]['_source']['s_title_name'],title['movie_name'])
        if clean_title_flag==1:
            insert_matched_title(source,3, f_mattched_list[0], title,'90')
    elif(response['hits']['total']>1):
        print(response)
                
          

def find_exact_match(source):
    count=0
    for title in query_title_matching_records():
        count +=1
        print(count, end='\r')
        response = elastic_exact_query(source,title['movie_name'],title['release_date'])
        response_rdate = response

        if(response['hits']['total']==1):
            insert_matched_title(source,1,response['hits']['hits'][0], title,'100')
        elif(response['hits']['total']==0):
            for release_date in query_other_release_dates(title['movie_id']):
                response_rdate = elastic_exact_query(source,title['movie_name'],release_date) 
                if(response_rdate['hits']['total']==1): 
                   # print('inserting stage 2-exact')   
                    insert_matched_title(source,2,response_rdate['hits']['hits'][0], title,'100')   
                elif(response_rdate['hits']['total']==0):
                    find_fuzzy_match_with_date(source,title,release_date)
                    #response_rdate = elastic_fuzzy_query(source,title['movie_name'],release_date)
                   # print(source,title['movie_name'],release_date)
                    #print(elastic_fuzzy_query(source,title['movie_name'],release_date))
                    #if(response_rdate['hits']['total']==1):
                       # print('inserting stage 2-fuzzy') 
                        #insert_matched_title(source,4,response_rdate['hits']['hits'][0], title,'100')
        elif(response['hits']['total'] > 1):
            print('find-exact-match > 1--->',response['hits']['hits'])

def replace_all(text, alist):
    for i in alist:
        text = text.replace(i, '')
    return text     

def clean_title(title_name,magic_title):
    title_name = title_name.upper()
    magic_title = magic_title.upper()
    print(title_name,magic_title)
    rules = cleaning_rules()
    for rule in rules:
        title_name = string_replace(rule,title_name)
        magic_title = string_replace(rule,magic_title)
    print(title_name,magic_title)
    return equal_title(magic_title,title_name)
  
  
def string_replace(rule,title):
    return re.sub(rule,' ',title)   
            

def equal_title(magic_title,source_title):
    magic_title = magic_title.strip().replace(' ','')
    source_title = source_title.strip().replace(" ","")
    if(magic_title==source_title):
        print("direct match")
        return 1
    elif(magic_title in source_title): 
        print("in match")
        return 1   

def cleaning_rules():
    l_rules = {r':',u'\(([0-9]{4})\)',r' AND ',r" THE ",r" THE",r", A",r' A ',r', AN',r' OF ',r' OR ',r"[-()\"#/&@;:'<>{}`+=~|.!?,]"}
    return l_rules  

def sources_11():
    sources_11 = {'MCAST'}
    return sources_11            


def main():
    print('calling.....')
    sources = sources_11()
    for source in sources:
        find_exact_match(source)
        #find_fuzzy_match(source)

if __name__ == '__main__':
    main()



