
# import pyspark
import sys
from collections import Counter, OrderedDict, defaultdict
import itertools
from itertools import islice, count, groupby
import pandas as pd
import os
import re
from operator import itemgetter
import nltk
from nltk.stem.porter import *
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from time import time
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import math
from functools import reduce
from google.cloud import storage
from inverted_index_gcp import *


import hashlib
def _hash(s):
    return hashlib.blake2b(bytes(s, encoding='utf8'), digest_size=5).hexdigest()


nltk.download('stopwords')


# Stopwords
english_stopwords = frozenset(stopwords.words('english'))
corpus_stopwords = ["category", "references", "also", "external", "links", 
                    "may", "first", "see", "history", "people", "one", "two", 
                    "part", "thumb", "including", "second", "following", 
                    "many", "however", "would", "became"]
RE_WORD = re.compile(r"""[\#\@\w](['\-]?\w){2,24}""", re.UNICODE)
all_stopwords = english_stopwords.union(corpus_stopwords)




# # Query Preparation


# tokenize & remove stop words.
def tokenize_and_remove_sw(text):
  tokens = [token.group() for token in RE_WORD.finditer(text.lower())]
  return [x for x in tokens if x not in all_stopwords]

# stemming
def stemming(tokens):
  stemmer = PorterStemmer()
  return [stemmer.stem(x) for x in tokens]

# reading postinglist




TUPLE_SIZE = 6       
TF_MASK = 2 ** 16 - 1 # Masking the 16 low bits of an integer
from contextlib import closing

client = storage.Client()
def read_posting_list(inverted, w, bucket_name):
  with closing(MultiFileReader()) as reader:
    locs = inverted.posting_locs[w]
    b = reader.read(locs, inverted.df[w] * TUPLE_SIZE, bucket_name)
    posting_list = []
    for i in range(inverted.df[w]):
      doc_id = int.from_bytes(b[i*TUPLE_SIZE:i*TUPLE_SIZE+4], 'big')
      tf = int.from_bytes(b[i*TUPLE_SIZE+4:(i+1)*TUPLE_SIZE], 'big')
      posting_list.append((doc_id, tf))
    return posting_list


# Search

# loading inverted index from title bucket
my_bucket = client.bucket('sean_bucket_title')
idx_title = pickle.loads(my_bucket.get_blob('postings_gcp/index_title.pkl').download_as_string())



def get_search_title(query):
    res = []
    filtered_query = tokenize_and_remove_sw(query)
    all_posting_lists = {}
    for term in np.unique(filtered_query):
      if term in idx_title.df:
        try:
            res = read_posting_list(idx_title, term, 'sean_bucket_title')
            for doc_id, amount in res:
                try:
                    all_posting_lists[doc_id] += 1
                except:
                    all_posting_lists[doc_id] = 1
        except Exception as e:
            print('error in title index occured - ', e)
    res= sorted(all_posting_lists, key=all_posting_lists.get, reverse=True)
    res = [(id, idx_title.title_dict[id]) for id in res]
    return(res)


# Search Body

# loading inverted index from body bucket
my_bucket = client.bucket('sean_bucket_body')
idx_body = pickle.loads(my_bucket.get_blob('postings_gcp/index_body.pkl').download_as_string())



# Cosine_Similarity {id:cosine score}
def cosine_similarity(search_query, index):
    """ Returns: {id:cosine score} """
    dict_cosine_sim = {}
    for term in search_query:
      if term in index.df.keys():
        pos_lst = read_posting_list(index,term, "sean_bucket_body")
        for doc_id, freq in pos_lst:
          if doc_id in dict_cosine_sim.keys():
            dict_cosine_sim[doc_id] += (freq/index.dl[doc_id])*(math.log(len(index.nf)/index.df[term],10))
          else:
            dict_cosine_sim[doc_id] = (freq/index.dl[doc_id])*(math.log(len(index.nf)/index.df[term],10))
    for doc_id in dict_cosine_sim.keys():
        dict_cosine_sim[doc_id] *= (1/len(search_query) * index.nf[doc_id])
    return dict_cosine_sim
 


# get list of top N ranked pairs (doc_id, score)
def get_top_n(sim_dict,N=3):
  return sorted([(doc_id,score) for doc_id, score in sim_dict.items()], key = lambda x: x[1],reverse=True)[:N]


def get_search_body(query):
    filtered_query = tokenize_and_remove_sw(query)
    cos_dct = cosine_similarity(filtered_query,idx_body)
    res = get_top_n(cos_dct,100)
    return [(id[0], idx_body.title_dict[id[0]]) for id in res]
  


# Search Anchor

my_bucket = client.bucket('sean_bucket_anchor')
idx_anchor = pickle.loads(my_bucket.get_blob('postings_gcp/index_anchor.pkl').download_as_string())



def get_search_anchor(query):
    res = []
    # if len(query) == 0:
    #   return jsonify(res)
    filtered_query = tokenize_and_remove_sw(query)
    all_posting_lists = {}
    for term in np.unique(filtered_query):
        if term in idx_anchor.df:
            pos = read_posting_list(idx_anchor, term, "sean_bucket_anchor")
            for doc_id, amount in pos:
                try:
                    all_posting_lists[doc_id] += 1
                except:
                    all_posting_lists[doc_id] = 1
    x= sorted(all_posting_lists, key=all_posting_lists.get, reverse=True)
    res = [(id, idx_anchor.title_dict[id]) for id in x if id in idx_anchor.title_dict]
    return res
  # return jsonify(res)


# Page rank


storage_client = storage.Client()
bucket_name = 'sean_bucket_title'
bucket = storage_client.get_bucket(bucket_name)



# load the pickle file from the bucket
pr = pickle.loads(bucket.get_blob('pageranks_dict.pkl').download_as_string())



pr_dct = {k:(lambda v:v/9913.728782160779)(v) for k,v in pr.items()}




def page_rank(wiki_id,page_rank_dict):
    lst_pagerank=[]
    for wikiid in wiki_id:
        try:
            lst_pagerank.append(float(page_rank_dict[wikiid]))
        except:
            lst_pagerank.append(0)
    
  
    return lst_pagerank


# Page view


pv = pickle.loads(bucket.get_blob('page_view.pkl').download_as_string())


pv_dct = {k:(lambda v:v/181126232)(v) for k,v in pv.items()}



def get_page_view(relevant_docs, wid2pv):
    return [(doc, wid2pv[doc]) if doc in wid2pv else (doc, 0) for doc in relevant_docs]

pv_max, pv_min, pv_mean = (max(pv), min(pv), np.mean(list(pv.values())))
pr_max, pr_min, pr_mean = (max(pr), min(pr), np.mean(list(pr.values())))



# BM25

avg_doclen= 319.5242353411845
N = 6348910



def calc_idf(list_of_tokens, N,index):
    idf = {}
    for term in list_of_tokens:
        if term in index.df.keys():
            n_ti = index.df[term]
            idf[term] = math.log(1 + (N - n_ti + 0.5) / (n_ti + 0.5))
    return idf
def calc_idf2(list_of_tokens, N,index):
    idf = {}
    for term in list_of_tokens:
        if term in index.df.keys():
            n_ti = index.df[term]
            idf[term] = math.log(N +1 /n_ti)
    return idf

def BM25(search_query, index, k, b,avg_doclen,N, threshHold=200):
    filtered_query = tokenize_and_remove_sw(search_query)
    bm_score = {}
    idf_dict = calc_idf(filtered_query, N, index)
    for term in filtered_query:
        if term in index.df.keys():
            pos_lst = read_posting_list(index,term, "sean_bucket_body")
            for doc_id, freq in pos_lst:
                B = 1-b + b*(index.dl[doc_id]/avg_doclen)
                idf= idf_dict[term]
                tf = freq/index.dl[doc_id]
                if doc_id in bm_score:
                    bm_score[doc_id]+= (idf*freq*(k+1))/((freq+B*k))
                else:
                    bm_score[doc_id] = (idf*freq*(k+1))/((freq+B*k))
    
    return dict(get_top_n(bm_score,threshHold))

my_bucket = client.bucket('sean_bucket_stem_title')
idx_title_stem = pickle.loads(my_bucket.get_blob('postings_gcp/index_stemmed_title.pkl').download_as_string())

# Combined Search

def search_title_helper(query):
    res = []
    filtered_query = tokenize_and_remove_sw(query)
    filtered_query = stemming(filtered_query)
    all_posting_lists = {}
    for term in np.unique(filtered_query):
      if term in idx_title_stem.df:
        try:
            res = read_posting_list(idx_title_stem, term, 'sean_bucket_stem_title')
            for doc_id, amount in res:
                try:
                    all_posting_lists[doc_id] += 1
                except:
                    all_posting_lists[doc_id] = 1
        except Exception as e:
            print('error in title index occured - ', e)
    return dict(get_top_n(all_posting_lists,200))



def search_anchor_helper(query):
    res = []
    filtered_query = tokenize_and_remove_sw(query)
    all_posting_lists = {}
    for term in np.unique(filtered_query):
        if term in idx_anchor.df:
            pos = read_posting_list(idx_anchor, term, "sean_bucket_anchor")
            for doc_id, amount in pos:
                try:
                    all_posting_lists[doc_id] += amount
                except:
                    all_posting_lists[doc_id] = amount
    return dict(get_top_n(all_posting_lists,200))



# 2nd best
def combined_Search_6_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                               N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr and doc_id in pv_dct :
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id] * pv_dct[doc_id]) / (score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])
    else:

        for doc_id, score in body_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * pr[doc_id] * pv_dct[doc_id]) / (score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
        for doc_id, score in anchor_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * w_anc
        for doc_id, score in title_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * w_title
    sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
    res = [(id, idx_anchor.title_dict[id]) for id in sorted_scores if id in idx_anchor.title_dict][:100]
    return res

# the best
def combined_Search_12_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                               N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)
    filtered_query=stemming(filtered_query)
    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr and doc_id in pv_dct :
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id] * pv_dct[doc_id]) / (score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])

    else:

        for doc_id, score in body_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * pr[doc_id] * pv_dct[doc_id]) / (score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
        for doc_id, score in anchor_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * w_anc
        for doc_id, score in title_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * w_title
    sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
    res = [(id, idx_body.title_dict[id]) for id in sorted_scores if id in idx_body.title_dict][:100]
    return res
