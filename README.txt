Introduction to pyspark coding.

The "main.py" file is a pyspark implementation for finding word co-occurrence using
Pointwise Mutual Information (PMI) score. The formula for calculating score used
is given by-

PMI(w1,w2) = log2(P(w1,w2)/(P(w1)xP(w2)))

where,
P(w1,w2) is ratio of number of documents where words w1 and w2 occur together to 
total number of documents
P(w1) is ratio of number of documents where word w1 occurs to total number of
documents.
P(w2) is ratio of number of documents where word w2 occurs to total number of
documents.

To run the file, use following command in python shell/terminal:

python3 main.py *path_to_file* *query_word* *k* *stopword_file*

where,
path_to_file - is the location of the input .txt file dataset
query_word - is the word for which co-occurrence needs to be calculated (w1)
k - Number of positively and negatively associated words to be printed
stopword_file - is the locatio of file containing all the stopwords to be removed

The output prints top k positively associated words and top k negatively associated 
words with the query word.

The link to stopword file is -
https://github.com/terrier-org/terrier-desktop/blob/master/share/stopword-list.txt