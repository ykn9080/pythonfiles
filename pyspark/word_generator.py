#!/bin/python3

import random
import string
import requests


import urllib.request
wordlist=42

def generate_word()->dict:
    word_url = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"
    response = urllib.request.urlopen(word_url)
    long_txt = response.read().decode()
    words = long_txt.splitlines()
    return{
        words
    }

def get_list_of_words():
    global wordlist
    if wordlist !=42:
        print("wordlist")
        return wordlist
    else:
        print("newwwww")
        response = requests.get(
            'https://www.mit.edu/~ecprice/wordlist.10000',
            timeout=10
        )

        string_of_words = response.content.decode('utf-8')

        list_of_words = string_of_words.splitlines()
        wordlist=list_of_words
    
        return list_of_words
if __name__=="__main__":
    # print(generate_word())
    words=get_list_of_words()
    n_random_words = [
        random.choice(words) for _ in range(3)
    ]
    print(n_random_words)
    
