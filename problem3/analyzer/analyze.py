# import os, json, time, itertools, re
# from collections import Counter
# from datetime import datetime, timezone

# def jaccard_similarity(doc1_words, doc2_words):
#     """Calculate Jaccard similarity between two documents."""
#     set1 = set(doc1_words)
#     set2 = set(doc2_words)
#     intersection = set1.intersection(set2)
#     union = set1.union(set2)
#     return len(intersection) / len(union) if union else 0.0

# WORD_RE = re.compile(r"[A-Za-z0-9]+")

# def now_iso_z():
#     return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

# def ngrams(tokens, n):
#     return [" ".join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]

# def main():
#     # wait for processor
#     done_flag = "/shared/status/process_complete.json"
#     while not os.path.exists(done_flag):
#         print("analyzer: waiting for process_complete.json ...", flush=True)
#         time.sleep(2)

#     os.makedirs("/shared/analysis", exist_ok=True)

#     # load processed docs
#     tokens_by_doc = {}
#     processed_dir = "/shared/processed"
#     for name in sorted(os.listdir(processed_dir)):
#         if not name.endswith(".json"):
#             continue
#         with open(os.path.join(processed_dir, name), "r", encoding="utf-8") as f:
#             data = json.load(f)
#         text = data.get("text", "")
#         tokens = [w.lower() for w in WORD_RE.findall(text)]
#         tokens_by_doc[name] = tokens

#     docs = list(tokens_by_doc.keys())
#     total_words = sum(len(t) for t in tokens_by_doc.values())

#     # global frequency
#     global_freq = Counter()
#     for t in tokens_by_doc.values():
#         global_freq.update(t)

#     # pairwise similarity (use provided jaccard_similarity)
#     similarities = []
#     for a, b in itertools.combinations(docs, 2):
#         sim = jaccard_similarity(tokens_by_doc[a], tokens_by_doc[b])
#         similarities.append({"doc1": a, "doc2": b, "similarity": sim})

#     # n-grams
#     bigrams = Counter()
#     trigrams = Counter()
#     for t in tokens_by_doc.values():
#         bigrams.update(ngrams(t, 2))
#         trigrams.update(ngrams(t, 3))

#     # readability (keep simple unless assignment gives a required formula)
#     avg_word_length = (sum(len(w) for w in global_freq.elements())/total_words) if total_words else 0.0
#     avg_sentence_length = 0.0  # TODO: if sentence counts are available in processed JSON, compute properly.
#     complexity_score = avg_word_length + (len(global_freq)/1000.0)  # TODO: or replace with the required formula.

#     report = {
#         "processing_timestamp": now_iso_z(),
#         "documents_processed": len(docs),
#         "total_words": total_words,
#         "unique_words": len(global_freq),
#         "top_100_words": [
#             {"word": w, "count": c, "frequency": (c/total_words if total_words else 0.0)}
#             for w, c in global_freq.most_common(100)
#         ],
#         "document_similarity": similarities,
#         "top_bigrams": [{"bigram": w, "count": c} for w, c in bigrams.most_common(50)],
#         "readability": {
#             "avg_sentence_length": avg_sentence_length,
#             "avg_word_length": avg_word_length,
#             "complexity_score": complexity_score
#         }
#     }

#     out_path = "/shared/analysis/final_report.json"
#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(report, f, indent=2, ensure_ascii=False)

#     print("analyzer: done", flush=True)

# if __name__ == "__main__":
#     main()



#!/usr/bin/env python3
import os
import re
import json
import time
import math
from glob import glob
from itertools import combinations, islice
from collections import Counter
from datetime import datetime, timezone

STATUS_DIR = "/shared/status"
PROCESSED_DIR = "/shared/processed"
ANALYSIS_DIR = "/shared/analysis"

def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

def jaccard_similarity(doc1_words, doc2_words):
    """Calculate Jaccard similarity between two documents."""
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def tokenize_lower(text):
    return re.findall(r'\b\w+\b', text.lower(), flags=re.UNICODE)

def sentences_in_text(text):
    parts = re.split(r'[.!?]+', text)
    return [p.strip() for p in parts if p.strip()]

def ngrams(tokens, n):
    if n <= 0:
        return []
    # sliding window
    return zip(*(islice(tokens, i, None) for i in range(n)))

def read_processed_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    print(f"[{utcnow_iso()}] Analyzer starting", flush=True)

    # Wait for processor completion
    proc_done = os.path.join(STATUS_DIR, "process_complete.json")
    while not os.path.exists(proc_done):
        print(f"Waiting for {proc_done}...", flush=True)
        time.sleep(2)

    os.makedirs(ANALYSIS_DIR, exist_ok=True)

    processed_files = sorted(glob(os.path.join(PROCESSED_DIR, "*.json")))
    docs = []
    for p in processed_files:
        try:
            data = read_processed_json(p)
            docs.append((os.path.basename(p), data))
        except Exception as e:
            print(f"Failed to read {p}: {e}", flush=True)

    # Aggregate statistics
    total_words = 0
    vocab_counter = Counter()
    bigram_counter = Counter()
    trigram_counter = Counter()

    # For similarity and readability
    doc_word_lists = {}
    all_sentences = 0
    total_sentence_len_words = 0
    total_word_len = 0

    for fname, d in docs:
        text = d.get("text", "")
        words = tokenize_lower(text)
        total_words += len(words)
        vocab_counter.update(words)
        # n-grams
        bigram_counter.update([' '.join(bg) for bg in ngrams(words, 2)])
        trigram_counter.update([' '.join(tg) for tg in ngrams(words, 3)])

        # per-doc word set for similarity
        doc_word_lists[fname] = words

        # readability
        sents = sentences_in_text(text)
        all_sentences += len(sents)
        total_sentence_len_words += sum(len(tokenize_lower(s)) for s in sents)
        total_word_len += sum(len(w) for w in words)

    documents_processed = len(docs)
    unique_words = len(vocab_counter)

    # Top 100 words
    top_100 = [
        {"word": w, "count": c, "frequency": (c / total_words) if total_words else 0.0}
        for w, c in vocab_counter.most_common(100)
    ]

    # Document similarity (pairwise Jaccard)
    similarity_list = []
    for (f1, _), (f2, _) in combinations(docs, 2):
        sim = jaccard_similarity(doc_word_lists.get(f1, []), doc_word_lists.get(f2, []))
        similarity_list.append({"doc1": f1, "doc2": f2, "similarity": round(sim, 6)})

    # Readability metrics
    avg_sentence_length = (total_sentence_len_words / all_sentences) if all_sentences else 0.0
    avg_word_length = (total_word_len / total_words) if total_words else 0.0
    # A simple composite "complexity_score"
    complexity_score = (avg_sentence_length * avg_word_length) if (avg_sentence_length and avg_word_length) else 0.0

    report = {
        "processing_timestamp": utcnow_iso(),
        "documents_processed": documents_processed,
        "total_words": total_words,
        "unique_words": unique_words,
        "top_100_words": top_100,
        "document_similarity": similarity_list,
        "top_bigrams": [{"bigram": bg, "count": c} for bg, c in bigram_counter.most_common(50)],
        "top_trigrams": [{"trigram": tg, "count": c} for tg, c in trigram_counter.most_common(50)],
        "readability": {
            "avg_sentence_length": round(avg_sentence_length, 3),
            "avg_word_length": round(avg_word_length, 3),
            "complexity_score": round(complexity_score, 3)
        }
    }

    out_path = os.path.join(ANALYSIS_DIR, "final_report.json")
    write_json(out_path, report)
    print(f"[{utcnow_iso()}] Analyzer complete -> {out_path}", flush=True)

if __name__ == "__main__":
    main()



