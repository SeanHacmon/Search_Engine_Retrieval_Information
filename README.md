# Search_Engine_Retrieval_Information

## All Functions Detailed

### Index_Creation 

##### Index Functions 

tokenize_and_remove_sw(text) - tokenize a string and remove stop words.

stemming(tokens) - get a list of tokenized terms and stem each token.

word_count_b(text, id) - tokenize the text + remove stop words, returns list of (term, [(doc_id, tf)] )  (used for body index).

word_count_t(text, id) - tokenize the text + remove stop words, returns list of (term, [(doc_id, 1)] )  (used for title index).

word_count_t_stem(text, id) - same as word_count_t just with stemming on the text.

doc_count_b(text, id) - creates a list of [(title, id)].

reduce_word_counts(unsorted_pl) - sorts posting lists by id.

calculate_df(postings) - calculate df for each term in posting list.

partion_posting_and_write(postings, bucket_name) - writes the index into the bucket.

calculate_DL(text, id)- tokenize a document and return it's length.

#### Creating index

CreateIndex(bucket_name, index_name) - uses all the data and creates an index in a folder called postings_gcp in the relevent bucket + 
                                       stores all bin/pkl files.
                                       
createStemmedIndex(bucket_name, index_name) - same as createIndex just apply stemming.

### PR_PV

##### Page rank upload
Creates a pandas dataframe of the page rank results + transforming into a dictionary and stores it at the title bucket as a 
pkl file.

##### Page view upload
Stores the Counter dicionary of the page view results in the title bucket as a pkl file.

### Search Tests 

Includes all the trys/tests of the search function with different weights and different approaches.

### TestSearchEngine

Generates a graph based on results we got from the test we ran on the train file that includes 30 querys.
we saved the first result and the last result called them lst1, lst12

### Inverted_index_gcp

A python file that have the Inverted index class and all it's functions that is needed to use and create an inverted index
each index will hold 5 dictionaries : 
  dl - {doc_id: doc length}
  df - {term : doc frequency}
  nf - {doc_id: normalized }
  term_total - {term: total frequency}
  posting_list - {term: posting list}
  posting_locs - {bin file : offset}
  title_dict - {doc_id: title}

### search_frontend

##### search()
Returns the best 100 results depending on usage of helper functions search_frontend_helper file.

##### search_title()
Returns the best results based on title index ordered by the title tf 

##### search_anchor() 
Returns the best results based on anchor index ordered by the anchor tf

##### search_body()
Returns the best 100 results using cosine similarity & tfidf

##### get_pagerank()
Returns list of values for each doc id in the given query based on pagerank pkl file that we load from the title bucket

##### get_pageview()
Returns list of values for each doc id in the given query based on pageview pkl file that we load from the title bucket

### search_frontend_helper

##### BM25
We used the formula : $\sum$ (idf * freq)*(k+1) / (freq*B*k)

##### Anchor Helper
Calculate score with the anchor index in a non-binary way giving better results then the normal anchor search.

##### Title Helper
Same as Title search just with stemming

##### Combined Search 
After 18 Test Cases we have received the best formula for the combined search.
Calculate the total score with the formula : BM25 + Page rank + Page views + Improved Anchor + Improved Title + Title Stemming.
While each the functions have received different weights for total score calculation.

## Validation Process


hold all the functions used for type of searches + the best 2 function trys of the search function
<img width="598" alt="Screen Shot 2023-01-15 at 21 51 55" src="https://user-images.githubusercontent.com/100718234/212563826-426e020f-4939-4981-bcfd-345c19f1a5a5.png">
