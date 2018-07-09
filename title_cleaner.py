import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

def replace_all(text, alist):
    for i in alist:
        text = text.replace(i, '')
    return text     

def clean_title(source_title,magic_title):
    source_title = source_title.upper()
    magic_title = magic_title.upper()
   # print("before cleaning    "+magic_title+ "Source: "+source_title)
    rules = cleaning_rules()
    for rule in rules:
        source_title = string_replace(rule,source_title)
        magic_title = string_replace(rule,magic_title)
    #print("after cleaning    "+magic_title+ "Source: "+source_title)
    return equal_title(magic_title,source_title)
  
def match_with_low_frequency_words(source_title,magic_title):
    print("low free   "+magic_title+ "   Source: "+source_title)
    stop_words = set(stopwords.words('english'))
    source_title_tokens = word_tokenize(source_title)
    magic_title_tokens = word_tokenize(magic_title)
    filtered_source_tokens = [w for w in source_title_tokens if not w in stop_words]
    matched_tokens = []
    for w in filtered_source_tokens:
        if len(magic_title_tokens)>3 and w in magic_title_tokens and len(w)>=3:
            matched_tokens.append(w)
    if(len(matched_tokens)>=2):
        return 1
     
  
def string_replace(rule,title):
    return re.sub(rule,' ',title)   
            

def equal_title(magic_title,source_title):
    match_flag = 0
    source_title_tokens = word_tokenize(source_title)
    magic_title_tokens = word_tokenize(magic_title)
    magic_title_1 = magic_title.strip().replace(' ','')
    source_title_1 = source_title.strip().replace(' ','')
    if(magic_title_1==source_title_1):
        match_flag=1
        return match_flag
    #elif(title_contains(magic_title_1,source_title_1)==1):   
    elif(len(source_title_tokens)==1 or len(magic_title_tokens)==1):
        print("before cleaning    "+magic_title+ "   Source: "+source_title)
        for rule in cleaning_rules_before_sigle_word_match():
            magic_title = string_replace(rule,magic_title) 
            source_title = string_replace(rule, source_title) 
            print("after cleaning    "+magic_title+ "   Source: "+source_title)
        magic_title = magic_title.strip().replace(' ','').upper()
        source_title = source_title.strip().replace(' ','').upper() 
        
        if(magic_title == source_title):
            match_flag = 4
            return match_flag

    elif(len(source_title_tokens)>1 and len(magic_title_tokens)>1):
        if(title_contains(magic_title_1,source_title_1)==1):
            match_flag=2
            return match_flag
        elif(match_with_low_frequency_words(source_title,magic_title)==1):
            match_flag=3
            return match_flag
    

def title_contains(magic_title, source_title):
    magic_title = magic_title.lower()
    source_title = source_title.lower()
    m_len = len(magic_title)
    s_len = len(source_title)
    if(m_len>s_len):
        if(source_title in magic_title):
            return 1
    else:
        if(magic_title in source_title):
            return 1

def clean_title_1(title):
    title = title.upper()
    rules = cleaning_rules()
    for rule in rules:
        title = string_replace(rule,title)
    return title          


def cleaning_rules():
    l_rules = {r':',u'\(([0-9/s]{4})\)',
    r' AND ',
    r' THE ',
    r'THE ',
    r' THE',
    r',$A ',
    r'^A',
    r' OF ',
    r' OR ',
    r'UNTITLED',
    r"[-()\"#/&@;:'<>{}`+=~|.!?,]"
    }
    return l_rules  

def cleaning_rules_before_sigle_word_match():
    rules = {
    r'(\b|(?i)(?<=_))ANNIVERSARY((?=_)|\b)',
    r'(\b|(?<=_))\s+((?=_)|\b)',
    r'(\b|(?<=_))[0-9]{4}((?=_)|\b)',
    r'(\b|(?<=_))[0-9]+(?i)TH((?=_)|\b)',
    r'(\b|(?<=_))(?i)3D((?=_)|\b)',
    r'(\b|(?<=_))(?i)I+((?=_)|\b)',
    r'(\b|(?<=_))(?i)S((?=_)|\b)'
    }
    return rules
 
