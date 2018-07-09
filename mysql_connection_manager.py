from mysql.connector import MySQLConnection, Error
from mysql_dbconfig import read_db_config
from datetime import date, datetime, timedelta
import hashlib

TABLE = 'TitleMatchingTest'

def connect():
    """ Connect to MySQL database """
    db_config = read_db_config()
    try:
        print('Connecting to MySQL database...')
        conn = MySQLConnection(**db_config)
        if conn.is_connected():
            print('connection established.')
        else:
            print('connection failed.')
    except Error as error:
        print(error)
    finally:
        conn.close()
        print('Connection closed.')

def query_title_matching_records():
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("select sourceid,sourcetitlename,sourcereleasedate,year(sourcereleasedate) as releaseyear from "+TABLE+" where SourceLookup like '%magic_id%'")
        magic_titles = list()
        for b in cursor:
            c = dict()
            c['movie_id'] = b[0]
            c['movie_name'] = b[1]
            c['release_date'] = b[2]
            c['release_year'] = b[3]
            magic_titles.append(c)
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
        return magic_titles   

def query_other_release_dates(movie_id):
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        query = "select date(release_date) from magic_release_dates_in_178 where magic_source_title_id="+movie_id
        #print(query)
        cursor.execute(query)
        other_release_dates = list()
        for date in cursor:
            other_release_dates.append(date[0])
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
        return other_release_dates            

def check_source_title_exists(source_hash):
    count = 0
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        #table = dbconfig['titleMatchingTable']
        cursor = conn.cursor()
        query = "select * from "+TABLE+" where SourceHash ='{}'".format(source_hash)
        cursor.execute(query)
        for c in cursor:
            if(c[19] == source_hash):
                count = 1
            else:
                count = 0    
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
    return count 


def imdb_refresh():
    try:
        dbconfig = read_db_config()
        imdb_list = list()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor(buffered=True)
        query = "select imdbid,imdbtitle,MagicSourceTitleID from TitleMatching where sourcename='Magic_Manual'"
        cursor.execute(query)
        for b in cursor:
            imdb_dict = dict()
            imdb_dict['imdbid']= b[0]
            imdb_dict['imdbtitle']= b[1]
            imdb_dict['MagicSourceTitleID']= b[2]
            imdb_list.append(imdb_dict)

        cursor.close()   

        for imdb in imdb_list:
            update_imdb(imdb)
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

def update_imdb(imdb):
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        #table = dbconfig['titleMatchingTable']
        cursor = conn.cursor()
     
        update_imdb = "UPDATE "+TABLE+" SET imdbid = %s, imdbtitle= %s WHERE MagicSourceTitleID = %s"
        imdb_data = (imdb['imdbid'],imdb['imdbtitle'],imdb['MagicSourceTitleID'])
        cursor.execute(update_imdb, imdb_data)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

        

def fetch_titles_for_fuzzy_match(sourcelookup):
    titles_for_fuzzy = list()
    try:
        dbconfig = read_db_config()
        #table = dbconfig['titleMatchingTable']
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        query = "select sourceid,sourcetitlename,sourcereleasedate,year(sourcereleasedate) as releaseyear,imdbid,imdbtitle from "+TABLE+" where SourceLookup='magic_id' and MagicSourceTitleID not in (select MagicSourceTitleID from "+TABLE+" where SourceLookup='"+sourcelookup+"') order by MagicSourceTitleID"
        #query="select sourceid,sourcetitlename,sourcereleasedate,year(sourcereleasedate) as releaseyear,imdbid,imdbtitle from TitleMatchingTest where SourceLookup='magic_id' and MagicSourceTitleID  in (30836  ,41759) order by MagicSourceTitleID"
        print(query)
        cursor.execute(query)
        for b in cursor:
            c = dict()
            c['movie_id'] = b[0]
            c['movie_name'] = b[1]
            c['release_date'] = b[2]
            c['release_year'] = b[3]
            c['imdbid']=b[4]
            c['imdbtitle']=b[5]
            titles_for_fuzzy.append(c)
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
        return titles_for_fuzzy
           

def insert_matched_title(source_name, stage,es_source, magic_title, confident):
    title = es_source['_source']
    hash_value = source_name+"_"+title['srcid']
    hashobj = hashlib.sha256(hash_value.encode('utf-8'))
    val_hex = hashobj.hexdigest()
   
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        #table = dbconfig['titleMatchingTable']
        cursor = conn.cursor()
        SourceID = source_name.lower()+'_id'

        if(source_name=='MaxusDCM'):
            title['s_release_date']=""
      
        if(check_source_title_exists(val_hex)==0):
            add_title = ("INSERT INTO `fma_data`.`"+TABLE+"` (`SourceName`,`SourceLookup`,`SourceID`,`SourceTitleName`,`SourceReleaseDate`,`MagicSourceTitleID`,`Stage`,`InsertedDateTime`,`TypeOfMatch`,`CreatedBy`,`Confidence`,`SourceHash`,`ESScore`,`ESDocID`,`imdbid`,`imdbtitle`)"\
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
            data_title = (title['s_source_name'], SourceID, title['srcid'], title['s_title_name'],title['s_release_date'],magic_title['movie_id'],stage,datetime.now().date(),'AutoTitleMatching','ATM',confident,val_hex,es_source['_score'], es_source['_id'],magic_title['imdbid'],magic_title['imdbtitle'])
            cursor.execute(add_title, data_title)
            conn.commit()
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


     
    
    

