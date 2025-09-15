#!/usr/bin/env python3
import sys, os, json, time, re
from urllib import request, error, parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from collections import Counter

# ---------- small helpers ----------
def now_iso_z():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

STOPWORDS = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
             'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
             'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
             'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
             'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it',
             'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
             'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other', 'some',
             'such', 'as', 'also', 'very', 'too', 'only', 'so', 'than', 'not'}

WORD_RE = re.compile(r"[A-Za-z0-9]+")
SENT_SPLIT = re.compile(r"[.!?]+")

def log_append(lines, msg):
    line = f"[{now_iso_z()}] {msg}"
    lines.append(line)
    print(line, flush=True)

# ---------- network ----------
def fetch_arxiv_xml(query, max_results, retries=3, wait_sec=3):
    base = "http://export.arxiv.org/api/query"
    url = f"{base}?search_query={parse.quote(query)}&start=0&max_results={max_results}"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with request.urlopen(url, timeout=20) as resp:
                # readable status check without getattr
                if hasattr(resp, "status"):
                    status_code = resp.status
                else:
                    status_code = 200
                if status_code == 429:
                    raise error.HTTPError(url, 429, "Too Many Requests", resp.headers, None)
                return resp.read()
        except error.HTTPError as e:
            last_err = e
            if e.code == 429 and attempt < retries:
                time.sleep(wait_sec)
                continue
            break
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(wait_sec)
                continue
            break
    raise last_err

# ---------- per-entry parsing ----------
def parse_entry(entry):
    ns = "{http://www.w3.org/2005/Atom}"

    id_text = entry.findtext(ns + "id") or ""
    arxiv_id = id_text.rsplit("/", 1)[-1] if "/" in id_text else id_text

    title = (entry.findtext(ns + "title") or "").strip()
    abstract = (entry.findtext(ns + "summary") or "").strip()
    published = (entry.findtext(ns + "published") or "").strip()
    updated = (entry.findtext(ns + "updated") or "").strip()

    authors = [a.findtext(ns + "name") or "" for a in entry.findall(ns + "author")]
    categories = [c.attrib.get("term", "") for c in entry.findall(ns + "category")]

    if not (arxiv_id and title and abstract):
        raise ValueError("missing required fields")

    words = WORD_RE.findall(abstract)
    words_lower = [w.lower() for w in words]

    total_words = len(words)
    unique_words = len(set(words_lower))

    sentences = [s for s in SENT_SPLIT.split(abstract) if s.strip()]
    total_sentences = len(sentences)
    avg_wps = (total_words / total_sentences) if total_sentences else 0.0
    avg_wlen = (sum(len(w) for w in words) / total_words) if total_words else 0.0

    uppercase_terms = sorted({w for w in WORD_RE.findall(abstract) if any(ch.isupper() for ch in w)})
    numeric_terms = sorted({w for w in WORD_RE.findall(abstract) if any(ch.isdigit() for ch in w)})
    hyphenated_terms = sorted(set(re.findall(r"\b\S*-\S*\b", abstract)))

    return {
        "arxiv_id": arxiv_id,
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "categories": categories,
        "published": published,
        "updated": updated,
        "abstract_stats": {
            "total_words": total_words,
            "unique_words": unique_words,
            "total_sentences": total_sentences,
            "avg_words_per_sentence": avg_wps,
            "avg_word_length": avg_wlen,
        },
        # for corpus aggregation
        "_freq": Counter(words_lower),
        "_docset": set(words_lower),
        "_tech": {
            "upper": set(uppercase_terms),
            "num": set(numeric_terms),
            "hyphen": set(hyphenated_terms),
        },
    }

# ---------- main ----------
def main():
    if len(sys.argv) != 4:
        print("Usage: python arxiv_processor.py <query> <max_results 1..100> <output_dir>")
        sys.exit(1)

    query, max_results_s, outdir = sys.argv[1], sys.argv[2], sys.argv[3]
    if not max_results_s.isdigit():
        print("max_results must be integer")
        sys.exit(1)
    max_results = int(max_results_s)
    if not (1 <= max_results <= 100):
        print("max_results must be between 1 and 100")
        sys.exit(1)
    os.makedirs(outdir, exist_ok=True)

    logs = []
    t0 = time.perf_counter()

    log_append(logs, f"Starting ArXiv query: {query}")

    # network
    try:
        xml_bytes = fetch_arxiv_xml(query, max_results)
    except Exception as e:
        log_append(logs, f"ERROR Network/API: {e}")
        with open(os.path.join(outdir, "processing.log"), "w", encoding="utf-8") as f:
            f.write("\n".join(logs) + "\n")
        sys.exit(1)

    # xml parse
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        log_append(logs, f"ERROR Invalid XML: {e}")
        with open(os.path.join(outdir, "processing.log"), "w", encoding="utf-8") as f:
            f.write("\n".join(logs) + "\n")
        sys.exit(1)

    ns = "{http://www.w3.org/2005/Atom}"
    entries = root.findall(ns + "entry")
    log_append(logs, f"Fetched {len(entries)} results from ArXiv API")

    papers = []
    corpus_freq = Counter()
    doc_freq = Counter()
    tech_upper, tech_num, tech_hyphen = set(), set(), set()
    lengths = []
    cat_all = Counter()

    for e in entries:
        try:
            p = parse_entry(e)
        except Exception as ex:
            log_append(logs, f"WARNING Skip paper: {ex}")
            continue

        # output object (no internal keys)
        papers.append({k: v for k, v in p.items() if not k.startswith("_")})

        # corpora aggregation
        corpus_freq.update(p["_freq"])
        for w in p["_docset"]:
            doc_freq[w] += 1
        tech_upper |= p["_tech"]["upper"]
        tech_num |= p["_tech"]["num"]
        tech_hyphen |= p["_tech"]["hyphen"]
        lengths.append(p["abstract_stats"]["total_words"])
        for c in p["categories"]:
            cat_all[c] += 1

        log_append(logs, f"Processing paper: {p['arxiv_id']}")

    # papers.json
    with open(os.path.join(outdir, "papers.json"), "w", encoding="utf-8") as f:
        json.dump(papers, f, indent=2, ensure_ascii=False)

    # corpus_analysis.json
    total_words = sum(lengths)
    top50 = [
        {"word": w, "frequency": int(corpus_freq[w]), "documents": int(doc_freq[w])}
        for w, _ in corpus_freq.most_common(50)
    ]
    corpus = {
        "query": query,
        "papers_processed": len(papers),
        "processing_timestamp": now_iso_z(),
        "corpus_stats": {
            "total_abstracts": len(papers),
            "total_words": total_words,
            "unique_words_global": len(set(corpus_freq.keys())),
            "avg_abstract_length": (total_words / len(papers)) if papers else 0.0,
            "longest_abstract_words": max(lengths) if lengths else 0,
            "shortest_abstract_words": min(lengths) if lengths else 0,
        },
        "top_50_words": top50,
        "technical_terms": {
            "uppercase_terms": sorted(tech_upper),
            "numeric_terms": sorted(tech_num),
            "hyphenated_terms": sorted(tech_hyphen),
        },
        "category_distribution": {k: int(v) for k, v in cat_all.items()},
    }
    with open(os.path.join(outdir, "corpus_analysis.json"), "w", encoding="utf-8") as f:
        json.dump(corpus, f, indent=2, ensure_ascii=False)

    # processing.log (with elapsed time)
    elapsed = time.perf_counter() - t0
    log_append(logs, f"Completed processing: {len(papers)} papers in {elapsed:.2f} seconds")
    with open(os.path.join(outdir, "processing.log"), "w", encoding="utf-8") as f:
        f.write("\n".join(logs) + "\n")

if __name__ == "__main__":
    main()

