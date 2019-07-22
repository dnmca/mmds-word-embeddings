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
5) Remove [stop words](https://github.com/andreyurkiv/mmds-word-embeddings/blob/master/data/stop_words).
6) Using word count and some manual work determine service Wikipedia words (used in markdown).
7) Remove [service words](https://github.com/andreyurkiv/mmds-word-embeddings/blob/master/data/service_words)

#### Cluster setup

Cluster for word2vec PySpark and Jupyter-Notebook was launched on [**GCP**](https://cloud.google.com/) running with [**Dataproc**](https://cloud.google.com/dataproc/). The initial `.xml` dump preprocessing was performed on [Datalab](https://cloud.google.com/datalab/) instance (since we were not able to run it locally because of memory consumption).

- Guides
  - [Official doc for cluster creation](https://cloud.google.com/dataproc/docs/guides/create-cluster)
  - [PySpark sentiment analysis](https://towardsdatascience.com/step-by-step-tutorial-pyspark-sentiment-analysis-on-google-dataproc-fef9bef46468)
  - [Jupyter notebook on dataproc cluster](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
  - [Word2vec model usage example](https://spark.apache.org/docs/2.2.0/api/python/_modules/pyspark/ml/feature.html#Word2Vec)

- Cluster configuration

| Node   | Replication factor | Memory | vCPU |
| ------ | ------------------ |:------:|:----:|
| Master | 1                  | 52 GB  | 2    |
| Worker | 3                  | 52 GB  | 2    |
| Total  | 4                  | 208 GB | 8    |

- Actions

1) Copy csv with articles from `gs` to cluster `hdfs`.
2) Download stop words and service words.
3) Download `n` articles (they were split into 90 `csv` files containing 10000 each).
4) Preprocess articles.
5) Create word2vec model from articles.
6) Store model on `gs` (cloud storage).

#### Word2Vec training

Word2Vec trains a model of Map(String, Vector), i.e. transforms a word into a code for further natural language processing or machine learning process.

We created ```Pipeline``` class to handle word2vec training.
This class includes:
1. ```init``` method, where we initialize dataframe, stop words, service words.

2.  ```preprocess``` method, which is used for preprocessing dataframe and calculating tokens. 

3.   ```fit``` method, which takes the number of samples for training model on, i.e. we create a spark dataframe with a part of the articles. 

While training we faced many java heap errors and rpc memory limit errors. We solved this by adding more RAM memory on our cluster setup.

In a reasonable time we got six trained word2vec models, trained on 10, 30, 50, 70, 90 and 110 thousands articles respectively. 

We discovered that vector size 100 for word2vec embeddings is sufficient for our model, because our dataset is relatively small.

Models were flushed on disk for evaluation step.

#### Evaluation

There is a [test set for word embeddings evaluation for Ukrainian language](https://raw.githubusercontent.com/lang-uk/vecs/master/test/test_vocabulary.txt). The size of test set is 23982 examples.

The idea of evaluation is to check relations between words. For example, word "king" relates to "queen" like "father" should relates to "mother".
So the idea is to find the word "mother". We use **gensim** library to find closest relations.

In our case we find top-10 closest words by relation (cosine dustance between embeddings) and check if the target word is in top-10. And then we calculate a precision of the embeddings.
We take our embedings which were trained on the different size of the datasets (10k, 30k, 50k, 70k, 90k, 110k, 150k articles) and compare with Ward2Vec embedings of [lang-ua](http://lang.org.ua/en/models/).

It occured that our the best embeddings have precision close to 37% and the lang-au embeddings has 49%. It can be explained by high variety of texts included into lang-ua corpus and special test examples which are not so good for wikipedia data, although lang-ua corpus include part of wikipedia articles as well. 
