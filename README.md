# Ukrainian Wiki word embeddings

## Motivation

There are exising embeddings trained on collected corpora of newswire, articles, fiction, juridical texts by the team of ukrainian researchers. We assumed that training similar embeddings on ukrainian wikipedia can give us comparable (or better) results, since wikipedia contains information from  larger amount of sources and fields of life. Besides it, we think that the possibility to process all articles at the same time by utilizing Big Data technologies can make data preparation process faster than manual one (combining od texts from different sources).

## Steps

#### Preprocessing

To prepare Wiki dump for further usage in training of Word2Vec model we performed few data engineering tasks:

1) Parsed `.xml` file with Wiki dump and splitted it into smaller manageble chunks with 10 thousand articles in each.
2) Read all chunks and concatenated them all into single data frame.
3) Remove all non-ukrainian letters, symbols, tags and special characters using regular expressions.
4) Tokenize texts.
5) Remove stop words ([data/stop_words.txt](https://github.com/andreyurkiv/mmds-word-embeddings/blob/master/data/stop_words)).
6) Using word count and some manual work determine service Wikipedia words (used in markdown).
7) Remove service words ([data/service_words.txt](https://github.com/andreyurkiv/mmds-word-embeddings/blob/master/data/service_words))

#### Cluster setup

Cluster for word2vec PySpark and Jupyter-Notebook was launched on [**GCP**]() running with [**Dataproc**](). The initial `.xml` dump preprocessing was performed on GCP Datalab instance (since we were not able to run it locally because of memory consumption).

- Config

| Node   | Replication factor | Memory | CPU |
| ------ | ------------------ |:------:|:---:|
| Master | 1                  | 26 GB  | 4 |
| Worker | 2                  | 13 GB  | 2 |

- Guide

- Articles

#### Word2Vec training


#### Evaluation

There is a test set for word embeddings evaluation https://raw.githubusercontent.com/lang-uk/vecs/master/test/test_vocabulary.txt for Ukrainian language.
The idea of evaluation is to check relations between words. For example, word "king" relates to "queen" like "father" should relates to "mother".
So the idea is to find the word "mother". We use **gensim** library to find closest relations.

In our case we find top-10 closest words by relation (cosine dustance between embeddings) and check if the target word is in top-10. And then we calculate a precision of the embeddings.
We take our embedings which were trained on the different size of the datasets (10k, 30k, 50k, 70k, 90k and 110k articles) and compare with Ward2Vec embedings of [lang-ua](http://lang.org.ua/en/models/).

It occured that our the best embeddings have precision close to 23% and the lang-au embeddings has 67%. It can be explained by high variety of texts included into lang-ua corpus and special test examples which are not so good for wikipedia data. Also lang-ua corpus include part of wikipedia articles as well. 
