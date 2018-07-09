from mysql_connection_manager import query_title_matching_records,check_source_title_exists,insert_matched_title,query_other_release_dates,fetch_titles_for_fuzzy_match
import title_cleaner as clean
import es_connection_manager as esconn


def elastic_exact_query(source, magic_title_name, magic_us_release_date):
    response = esconn.get_elastic_conn().search(index=source.lower(), body={
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
    response = esconn.get_elastic_conn().search(index=source.lower(), body={
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


def elastic_fuzzy_query_with_release_year(source, magic_title_name, release_year):
    query = {
       "min_score" : 2,
        "query": {
            "bool": {
                "must": [
                    {"match": {"s_title_name": magic_title_name}},
                    {"match": {"s_release_year":release_year}},
                    {"match": {"s_source_name": source}}
                ]
            }
        }
    }
    response = esconn.get_elastic_conn().search(index=source.lower(), body=query)
    return response    

def elastic_fuzzy_query_with_only_title(source, magic_title_name):
    query = {
       "min_score" : 5,
        "query": {
            "bool": {
                "must": [
                    {"match": {"s_title_name": magic_title_name}},
                    {"match": {"s_source_name": source}}
                ]
            }
        }
    }
    response = esconn.get_elastic_conn().search(index=source.lower(), body=query)
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
            if clean.clean_title(f_mattched_list[0]['_source']['s_title_name'],title['movie_name']):
                insert_matched_title(source,3, f_mattched_list[0], title,'90')
      

def insert_if_matched_with_one_title(source,stage,response,title,confident):
    if(response['hits']['total']==1):
            insert_matched_title(source,stage,response['hits']['hits'][0], title,confident)
            return 1

def insert_title(source,stage,source_title,title,confident):
    insert_matched_title(source,stage,source_title,title,confident)

def find_match_with_all_release_dates_in_178(source,response,title):
    match_flag = 0
    for release_date in query_other_release_dates(title['movie_id']):
                response = elastic_exact_query(source,title['movie_name'],release_date) 
                match_flag = insert_if_matched_with_one_title(source,"Ex_RD",response,title,100)
                if match_flag !=1:
                    find_fuzzy_match_with_cleaned_title_and_release_date(source=source,title=title,release_date=release_date)

def find_fuzzy_match_with_cleaned_title_and_release_date(source,title,release_date):   
    match_flag = 0         
    response = elastic_fuzzy_query(source,title['movie_name'], release_date)
    match_flag=find_match_from_fuzzy_results(source,"FUZ_RD",response,title,80)
    #Write log if the match_flag is one.

def find_fuzzy_match_with_cleaned_title_and_release_year(source,title,range):
    match_flag = 0 
    if range:
        for r in range(-1, 2, 1):
            response = elastic_fuzzy_query_with_release_year(source,title['movie_name'],title['release_year']+r)
            if(r==0):
                match_flag=find_match_from_fuzzy_results(source,"FUZ_RY",response,title,75)
            else:
                match_flag=find_match_from_fuzzy_results(source,"FUZ_RY_("+r+")",response,title,50)    
    else:
        response = elastic_fuzzy_query_with_release_year(source,title['movie_name'],title['release_year'])
        match_flag=find_match_from_fuzzy_results(source,"FUZ_RY",response,title,75)
    #Write log if the match_flag is one.

def find_fuzzy_match_with_only_title(source,title):
    match_flag = 0         
    response = elastic_fuzzy_query_with_only_title(source,title['movie_name'])
    match_flag=find_match_from_fuzzy_results(source,"FUZ",response,title,55)

    
def find_match_from_fuzzy_results(source,stage,response,title,confident):
    f_mattched_list = list()
    match_flag = 0
    source_title_name=""
    
    for hit in response['hits']['hits']:
        f_mattched_list.append(hit)
        source_title_name=f_mattched_list[0]['_source']['s_title_name' ]   
    if(len(f_mattched_list)>0):    
        match_flag = clean.clean_title(source_title_name,title['movie_name'])
        if match_flag==1:
            insert_title(source,stage,f_mattched_list[0],title,confident)
            return match_flag
        elif match_flag==2:
            insert_title(source,stage+"_CONM",f_mattched_list[0],title,confident-25)
            return match_flag
        elif match_flag==3:
            insert_title(source,stage+"_WORM",f_mattched_list[0],title,confident-50)       
            return match_flag
        elif match_flag==4:
            insert_title(source,stage+"_SWM_AFC",f_mattched_list[0],title,confident)    


def find_match_with_relese_date(source):
    count=0
    match_flag=0
    for title in fetch_titles_for_fuzzy_match(source.lower()+'_id'):
        count +=1
        print(count, end='\r')
        response = elastic_exact_query(source,title['movie_name'],title['release_date'])
        match_flag = insert_if_matched_with_one_title(source,"EX_RD",response,title,100)
        if match_flag!=1:
            find_match_with_all_release_dates_in_178(source,response,title)

            
def find_match_with_release_year(source,range_flag):
    count=0
    for title in fetch_titles_for_fuzzy_match(source.lower()+'_id'):
        count +=1
        print(count, end='\r')
        find_fuzzy_match_with_cleaned_title_and_release_year(source,title,range_flag)

def find_match_with_title_name(source):
    count=0
    for title in fetch_titles_for_fuzzy_match(source.lower()+'_id'):
        count +=1
        print(count, end='\r')
        find_fuzzy_match_with_only_title(source,title)       

def find_match(source,pattern):
    if(pattern=='RD'):
        find_match_with_relese_date(source)
    elif(pattern=="RY"):
        find_match_with_release_year(source,0)           
           
    
def fetch_sources_one_to_one():
    sources_11 = {'gacampaignperformance'}
    return sources_11            

def main():
    
    sources = fetch_sources_one_to_one()
    for source in sources:
        #find_match_with_relese_date(source)
        #find_match_with_release_year(source,0)
        find_match_with_title_name(source)
      

if __name__ == '__main__':
    main()



