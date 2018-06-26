from mysql.connector import MySQLConnection, Error
from mysql_dbconfig import read_db_config
from datetime import date, datetime, timedelta
import hashlib

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
        cursor.execute("select * from TMTesting where SourceLookup like '%magic_id%'")
        magic_titles = list()
        for b in cursor:
            c = dict()
            c['movie_id'] = b[3]
            c['movie_name'] = b[4]
            c['release_date'] = b[5]
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
        cursor = conn.cursor()
        cursor.execute("select * from TMTesting where SourceHash ='{}'".format(source_hash))
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

def fetch_titles_for_fuzzy_match(sourcelookup):
    titles_for_fuzzy = list()
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("select * from TMTesting where SourceLookup='magic_id'and magicid not in (select magicid from TMTesting where SourceLookup='"+sourcelookup+"') order by magicid")
        for b in cursor:
            c = dict()
            c['movie_id'] = b[3]
            c['movie_name'] = b[4]
            c['release_date'] = b[5]
            titles_for_fuzzy.append(c)
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
        return titles_for_fuzzy
           

def insert_matched_title(source_name, stage,es_source, magic_title, confident):
    title = es_source['_source']
    hashobj = hashlib.sha256(title['srcid'].encode('utf-8'))
    val_hex = hashobj.hexdigest()
   
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        SourceID = source_name.lower()+'_id'

        if(check_source_title_exists(val_hex)==0):
            add_title = ("INSERT INTO `fma_data`.`TMTesting` (`SourceName`,`SourceLookup`,`SourceID`,`SourceTitleName`,`SourceReleaseDate`,`MagicID`,`Stage`,`InsertedDateTime`,`TypeOfMatch`,`CreatedBy`,`Confidence`,`SourceHash`,`ESScore`,`ESDocID`)"\
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
            data_title = (title['s_source_name'], SourceID, title['srcid'], title['s_title_name'],title['s_release_date'],magic_title['movie_id'],stage,datetime.now().date(),'AutoTitleMatching','ATM',confident,val_hex,es_source['_score'], es_source['_id'] )
            cursor.execute(add_title, data_title)
            conn.commit()
    except Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

    
     
    
    

