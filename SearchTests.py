import search_frontend_helper
def combined_Search_first_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                              N):
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)
    combined_scores = defaultdict(int)

    for doc_id, score in body_scores.items():
        combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])

    for doc_id, score in anchor_scores.items():
        combined_scores[doc_id] *= score * w_anc

    sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
    res = [(id, idx_anchor.title_dict[id]) for id in sorted_scores if id in idx_anchor.title_dict][:100]
    return res


# the best one
def combined_Search_second_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                               N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])

        for doc_id, score in title_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_title * pr[doc_id]) / (score + pr[doc_id])

    else:
        for doc_id, score in body_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])

        for doc_id, score in anchor_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * w_anc

    sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
    res = [(id, idx_anchor.title_dict[id]) for id in sorted_scores if id in idx_anchor.title_dict][:100]
    return res


# In[ ]:
# the best
def combined_Search_third_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                              N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])

        for doc_id, score in title_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_title * pr[doc_id]) / (score + pr[doc_id])

    else:

        for doc_id, score in body_scores.items():
            if doc_id in pr:
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
def combined_Search_4_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                          N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])

    else:

        for doc_id, score in body_scores.items():
            if doc_id in pr:
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


def combined_Search_5_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                          N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id] * pv_dct[doc_id]) / (
                            score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])
    # for doc_id, score in title_scores.items():
    #    if doc_id in pr:
    #       combined_scores[doc_id] += (2 * score * w_title * pr[doc_id]) / (score + pr[doc_id])

    else:
        for doc_id, score in body_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                combined_scores[doc_id] += (2 * score * pr[doc_id] * pv_dct[doc_id]) / (
                            score + pr[doc_id] + pv_dct[doc_id])
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
# new best


def combined_Search_7_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                          N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (score * w_anc * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (score * w_anc * pr[doc_id] * pv_dct[doc_id]) / (
                                score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (score * w_anc * pr[doc_id]) / (score + pr[doc_id])

        for doc_id, score in body_scores.items():
            combined_scores[doc_id] *= score * 2

    else:

        for doc_id, score in body_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (3 * score * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (3 * score * pr[doc_id] * pv_dct[doc_id]) / (
                                score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (3 * score * pr[doc_id]) / (score + pr[doc_id])
        for doc_id, score in anchor_scores.items():
            combined_scores[doc_id] *= score * 2
        for doc_id, score in title_scores.items():
            combined_scores[doc_id] *= score * 2
    sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
    res = [(id, idx_anchor.title_dict[id]) for id in sorted_scores if id in idx_anchor.title_dict][:100]
    return res


def combined_Search_8_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                          N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    for doc_id, score in body_scores.items():
        page_view = pv.get(doc_id, 1)
        combined_scores[doc_id] += (2 * score * page_view) / (score + page_view)

    for doc_id, score in anchor_scores.items():
        combined_scores[doc_id] *= score * w_anc

    sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
    res = [(id, idx_anchor.title_dict[id]) for id in sorted_scores if id in idx_anchor.title_dict][:100]
    return res


def combined_Search_9_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                          N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * pr[doc_id] * pv_dct[doc_id]) / (
                            score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
        for doc_id, score in title_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * w_title

    else:
        for doc_id, score in body_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * pr[doc_id] * pv_dct[doc_id]) / (
                            score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
        for doc_id, score in anchor_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * 2
        for doc_id, score in title_scores.items():
            if doc_id in pr:
                combined_scores[doc_id] *= score * w_title
    sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
    res = [(id, idx_anchor.title_dict[id]) for id in sorted_scores if id in idx_anchor.title_dict][:100]
    return res


def consider_pr_pv(docs, pagerank_w, pageview_w, pagerank_dict, pageview_dict):
    sorted_by_weigts = {}
    for doc_id, score in docs.items():
        # normalize by minmax
        pageview = (pageview_dict.get(doc_id, 680) - pv_mean) / (pv_max - pv_min)
        pagerank = (pagerank_dict.get(doc_id, 1) - pr_mean) / (pr_max - pr_min)
        sorted_by_weigts[doc_id] = pagerank_w * pagerank + pageview_w * pageview
        # no need to use heapq, 40 values to sort
    return sorted(list(sorted_by_weigts.items()), key=lambda x: x[1], reverse=True)


def combined_Search_10_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                           N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N, 100)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id] * pv_dct[doc_id]) / (
                                score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])

        sorted_scores = sorted(combined_scores, key=combined_scores.get, reverse=True)
        res = [(id, idx_body.title_dict[id]) for id in sorted_scores if id in idx_body.title_dict][:100]
        return res
    else:
        first_40 = dict(list(body_scores.items())[:40])
        last_60 = dict(list(body_scores.items())[40:])
        # reorder results according to page rank and page view (with weights)
        reorder_result_40 = consider_pr_pv(first_40, pagerank_w=0.6, pageview_w=0.4, pagerank_dict=pr,
                                           pageview_dict=pv)
        # add titles and combine
        res = [(id, idx_body.title_dict[id]) for id in reorder_result_40 if id in idx_body.title_dict]
        res += [(int(id), idx_body.title_dict[id]) for id in last_60]
        return res


def combined_Search_11_try(title_index, body_index, anchor_index, query, k, b, w_anc, w_body, w_title, avg_doclen,
                           N):
    filtered_query = tokenize_and_remove_sw(query)
    title_scores = search_title_helper(query)
    body_scores = BM25(query, body_index, k, b, avg_doclen, N)
    anchor_scores = search_anchor_helper(query)

    combined_scores = defaultdict(int)
    if len(filtered_query) == 1:
        for doc_id, score in anchor_scores.items():
            if doc_id in pr and doc_id in pv_dct:
                if pr_dct[doc_id] > pv_dct[doc_id]:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])
                else:
                    combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id] * pv_dct[doc_id]) / (
                                score + pr[doc_id] + pv_dct[doc_id])
            elif doc_id in pr:
                combined_scores[doc_id] += (2 * score * w_anc * pr[doc_id]) / (score + pr[doc_id])

    else:
        if len(filtered_query) == 2:
            for doc_id, score in title_scores.items():
                if doc_id in pr and doc_id in pv_dct:
                    if pr_dct[doc_id] > pv_dct[doc_id]:
                        combined_scores[doc_id] += (2 * score * w_title * pr[doc_id]) / (score + pr[doc_id])
                    else:
                        combined_scores[doc_id] += (2 * score * w_title * pr[doc_id] * pv_dct[doc_id]) / (
                                score + pr[doc_id] + pv_dct[doc_id])
                elif doc_id in pr:
                    combined_scores[doc_id] += (2 * score * w_title * pr[doc_id]) / (score + pr[doc_id])
        else:
            for doc_id, score in body_scores.items():
                if doc_id in pr and doc_id in pv_dct:
                    if pr_dct[doc_id] > pv_dct[doc_id]:
                        combined_scores[doc_id] += (2 * score * pr[doc_id]) / (score + pr[doc_id])
                    else:
                        combined_scores[doc_id] += (2 * score * pr[doc_id] * pv_dct[doc_id]) / (
                                    score + pr[doc_id] + pv_dct[doc_id])
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
