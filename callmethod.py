from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
 



def main():
    example_sent = "the purge 3"
    
    stop_words = set(stopwords.words('english'))
    
    word_tokens = word_tokenize(example_sent)
    
    filtered_sentence = [w for w in word_tokens if not w in stop_words]
    
    filtered_sentence = []
    
    for w in word_tokens:
        if w not in stop_words:
            filtered_sentence.append(w)
    
    print(word_tokens)
    print(filtered_sentence)

if __name__ == '__main__':
    main()
    