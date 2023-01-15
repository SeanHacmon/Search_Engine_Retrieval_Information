# Search_Engine_Retrieval_Information

## Index_Creation 

### Index Functions 

tokenize_and_remove_sw(text) - tokenize a string and remove stop words

stemming(tokens) - get a list of tokenized terms and stem each token.

word_count_b(text, id) - tokenize the text + remove stop words, returns list of (term, [(doc_id, tf)] )  (used for body index)

word_count_t(text, id) - tokenize the text + remove stop words, returns list of (term, [(doc_id, 1)] )  (used for title index)

word_count_t_stem(text, id) - same as word_count_t just with stemming on the text

doc_count_b(text, id) - creates a list of [(title, id)]

reduce_word_counts(unsorted_pl) - sorts posting lists by id

calculate_df(postings) - calculate df for each term in posting list

partion_posting_and_write(postings, bucket_name) - writes the index into the bucket

calculate_DL(text, id)- tokenize a document and return it's length

### Creating index

createIndex(bucket



