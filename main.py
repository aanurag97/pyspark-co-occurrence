# Importing necessary libraries
from pyspark import SparkContext
import re
import math
import sys
import os


# Function for removing digits and punctuations from word
def prep(lis):
    words = re.split(r'[^A-Za-z]+', lis)
    #words = re.sub(r"[^a-zA-Z\s]", "", lis)
    #words = words.split()
    out = [word.lower() for word in words if word != '']
    return (set(out))


# Function for counting occurance of given word
def g_func(lis, w):
    if w in lis:
        return (w, 1)
    else:
        return (w, 0)


# Function for counting co-occurance
def c_func(lis, w):
    if w in lis:
        lis = [item for item in lis if item != w]
        for item in lis:
            yield (item, [1, 1])
    else:
        for item in lis:
            yield (item, [1, 0])

def main():
    # All inputs in corresponding order
    file_path = sys.argv[1]
    g_word = sys.argv[2]
    top_k = int(sys.argv[3])
    file_path2 = sys.argv[4]


    # Reading and counting number of documents
    sc = SparkContext("local", "Filter app")
    file_rdd = sc.textFile(file_path)
    docs_rdd = file_rdd.map(lambda x: prep(x))
    num_doc = docs_rdd.count()


    # Counting given word frequency
    g_word_rdd = docs_rdd.map(lambda x: g_func(x, g_word.lower()))
    num_gword = g_word_rdd.values().sum()


    # Reading stopwords list
    stopwords_file = open(file_path2, 'r')
    stopwords_list = stopwords_file.read()
    stopwords = stopwords_list.split("\n")


    # Removing stopwords from document
    rem_stopwords_rdd = docs_rdd.map(lambda x: [item for item in x if item not in stopwords])
    # Counting co-occurance frequency
    occur_rdd = rem_stopwords_rdd.flatMap(lambda x: c_func(x, g_word.lower()))
    # Summing co-occurance values
    count_rdd = occur_rdd.reduceByKey(lambda a,b: [a[0]+b[0], a[1]+b[1]])
    # Filtering out 0 occurance words
    fcount_rdd = count_rdd.filter(lambda x: x[1][1] != 0)


    # Finding PMI scores and ordering them
    word_count_map = fcount_rdd.map(lambda x: (x[0], math.log2((x[1][1]*num_doc)/(x[1][0]*num_gword))))
    pos_words = word_count_map.top(top_k, key = lambda x: x[1])
    neg_words = word_count_map.takeOrdered(top_k, key = lambda x: x[1])


    # Printing the results
    print("Positively Associated words with given word are: ")
    for i in pos_words:
        print(i)

    print("Negatively Associated words with given word are: ")
    for i in neg_words:
        print(i)
    #for i in neg_words:
        #if i[1] < 0:
            #print(i)

if __name__ == "__main__":
    main()
