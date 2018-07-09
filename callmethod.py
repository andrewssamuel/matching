from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
 



def main():
    for i in range(-1, 2, 1):
        print(i)

if __name__ == '__main__':
    main()



           
'''
def find_fuzzy_match_with_release_dates(source,title,release_date):
    response = elastic_fuzzy_query(source,title['movie_name'], release_date)
    #if(response['hits']['total']==1):
    f_mattched_list = list()
    clean_title_flag = 0
    for hit in response['hits']['hits']:
        f_mattched_list.append(hit)
    if(len(f_mattched_list)>0):    
        clean_title_flag = clean.clean_title(f_mattched_list[0]['_source']['s_title_name'],title['movie_name'])
    if clean_title_flag==1:
        insert_matched_title(source,3, f_mattched_list[0], title,'90')

def find_fuzzy_match_with_release_year(source,title):
    response = elastic_fuzzy_query_with_release_year(source,title['movie_name'],title["release_year"]) 
    #if(response['hits']['total']==1):
    f_mattched_list = list()
    clean_title_flag = 0
    for hit in response['hits']['hits']:
        f_mattched_list.append(hit)
    if(len(f_mattched_list)>0):    
        clean_title_flag = clean.clean_title(f_mattched_list[0]['_source']['s_title_name'],title['movie_name'])
    if clean_title_flag==1:
        insert_matched_title(source,5, f_mattched_list[0], title,'75')   

   def find_exact_match(source):
    count=0
    for title in query_title_matching_records():
        count +=1
        print(count, end='\r')
        response = elastic_exact_query(source,title['movie_name'],title['release_date'])
        response_rdate = response
        if(response['hits']['total']==1):
            insert_matched_title(source,1,response['hits']['hits'][0], title,'100')
        elif(response['hits']['total']!=1):
            for release_date in query_other_release_dates(title['movie_id']):
                response_rdate = elastic_exact_query(source,title['movie_name'],release_date) 
                if(response_rdate['hits']['total']==1): 
                    insert_matched_title(source,2,response_rdate['hits']['hits'][0], title,'100')   
                elif(response_rdate['hits']['total']!=1):
                    #Calling the fuzzy search with all the release dates under the territory 178
                    find_fuzzy_match_with_release_dates(source=source,title=title,release_date=release_date)

'''   
    