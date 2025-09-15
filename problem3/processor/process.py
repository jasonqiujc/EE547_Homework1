# import os, json, time, re
# from datetime import datetime, timezone

# def strip_html(html_content):
#     """Remove HTML tags and extract text."""
#     # Remove script and style elements
#     html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
#     html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
#     # Extract links before removing tags
#     links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
#     # Extract images
#     images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
#     # Remove HTML tags
#     text = re.sub(r'<[^>]+>', ' ', html_content)
    
#     # Clean whitespace
#     text = re.sub(r'\s+', ' ', text).strip()
    
#     return text, links, images


# WORD_RE = re.compile(r"[A-Za-z0-9]+")
# SENT_RE = re.compile(r"[.!?]+")

# def now_iso_z():
#     return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

# def compute_stats(text: str) -> dict:
#     """You may implement this or reuse any provided helper from the assignment."""
#     words = WORD_RE.findall(text)
#     sentences = [s for s in SENT_RE.split(text) if s.strip()]
#     wc = len(words)
#     sc = len(sentences)
#     # a very simple paragraph proxy (OK for baseline unless assignment gives its own)
#     paragraphs = max(1, sc // 3)
#     avg_word_len = (sum(len(w) for w in words) / wc) if wc else 0.0
#     return {
#         "word_count": wc,
#         "sentence_count": sc,
#         "paragraph_count": paragraphs,
#         "avg_word_length": avg_word_len
#     }

# def main():
#     # wait for fetcher to finish
#     done_flag = "/shared/status/fetch_complete.json"
#     while not os.path.exists(done_flag):
#         print("processor: waiting for fetch_complete.json ...", flush=True)
#         time.sleep(2)

#     os.makedirs("/shared/processed", exist_ok=True)
#     os.makedirs("/shared/status", exist_ok=True)

#     raw_dir = "/shared/raw"
#     for name in sorted(os.listdir(raw_dir)):
#         if not name.endswith(".html"):
#             continue
#         path = os.path.join(raw_dir, name)
#         with open(path, "rb") as f:
#             html = f.read().decode("utf-8", errors="ignore")

#         # ---- use provided function (DO NOT MODIFY) ----
#         text, links, images = strip_html(html)

#         # ---- your part: compute stats + write JSON ----
#         stats = compute_stats(text)  # TODO: if the assignment provides a stats func, call that instead.

#         out_obj = {
#             "source_file": name,
#             "text": text,
#             "statistics": stats,
#             "links": links,
#             "images": images,
#             "processed_at": now_iso_z()
#         }
#         out_path = os.path.join("/shared/processed", name.replace(".html", ".json"))
#         with open(out_path, "w", encoding="utf-8") as wf:
#             json.dump(out_obj, wf, indent=2, ensure_ascii=False)

#     # write stage-complete flag
#     with open("/shared/status/process_complete.json", "w", encoding="utf-8") as f:
#         json.dump({"timestamp": now_iso_z()}, f)

#     print("processor: done", flush=True)

# if __name__ == "__main__":
#     main()


#!/usr/bin/env python3
import os
import re
import json
import time
from glob import glob
from datetime import datetime, timezone

STATUS_DIR = "/shared/status"
RAW_DIR = "/shared/raw"
PROCESSED_DIR = "/shared/processed"

def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

def strip_html(html_content):
    """Remove HTML tags and extract text."""
    # Remove script and style elements
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

    # Extract links before removing tags
    links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)

    # Extract images
    images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)

    # Convert common block-level tags to newlines to keep some structure
    block_tags = [
        r'</p>', r'<br\s*/?>', r'</div>', r'</li>', r'</h[1-6]>', r'</tr>'
    ]
    for tag in block_tags:
        html_content = re.sub(tag, '\n', html_content, flags=re.IGNORECASE)

    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)

    # Normalize whitespace: collapse multiple newlines first, then spaces
    text = re.sub(r'\r', '\n', text)
    text = re.sub(r'\n{2,}', '\n\n', text)   # keep paragraph boundaries as double-newline
    text = re.sub(r'[ \t\f\v]+', ' ', text)
    # Trim lines and collapse accidental spaces around newlines
    text = '\n'.join(line.strip() for line in text.split('\n'))
    text = re.sub(r'\n{3,}', '\n\n', text).strip()

    return text, links, images

def tokenize_words(text):
    # Alphanumeric words (ASCII + underscores); for Unicode letters/digits use \w with re.UNICODE
    return re.findall(r'\b\w+\b', text, flags=re.UNICODE)

def split_sentences(text):
    # Split on ., !, ? (one or more), keep non-empty segments
    parts = re.split(r'[.!?]+', text)
    return [p.strip() for p in parts if p.strip()]

def count_paragraphs_from_text_preserving_double_newlines(text):
    # Paragraphs are separated by double newlines (introduced in strip_html)
    if not text.strip():
        return 0
    # Split on two or more newlines
    paras = re.split(r'\n{2,}', text.strip())
    paras = [p for p in paras if p.strip()]
    return len(paras)

def process_html_file(path):
    fname = os.path.basename(path)
    try:
        with open(path, 'rb') as f:
            raw = f.read()
        # decode with best-effort
        html = raw.decode('utf-8', errors='ignore')
        text, links, images = strip_html(html)

        words = tokenize_words(text)
        sentences = split_sentences(text)
        paragraph_count = count_paragraphs_from_text_preserving_double_newlines(text)

        word_count = len(words)
        sentence_count = len(sentences)
        avg_word_len = round(sum(len(w) for w in words) / word_count, 3) if word_count else 0.0

        out = {
            "source_file": fname,
            "text": text,
            "statistics": {
                "word_count": word_count,
                "sentence_count": sentence_count,
                "paragraph_count": paragraph_count,
                "avg_word_length": avg_word_len
            },
            "links": links,
            "images": images,
            "processed_at": utcnow_iso()
        }
        return out, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    print(f"[{utcnow_iso()}] Processor starting", flush=True)

    # Wait for fetcher completion status
    fetch_done = os.path.join(STATUS_DIR, "fetch_complete.json")
    while not os.path.exists(fetch_done):
        print(f"Waiting for {fetch_done}...", flush=True)
        time.sleep(2)

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(STATUS_DIR, exist_ok=True)

    html_files = sorted(glob(os.path.join(RAW_DIR, "*.html")))
    results = []
    success = 0
    failed = 0

    for idx, path in enumerate(html_files, 1):
        print(f"Processing {path}...", flush=True)
        out, err = process_html_file(path)
        if err is None:
            # Derive page_N.json by matching the page_N.html if possible, else use index
            base = os.path.basename(path)
            m = re.match(r'(page_\d+)\.html$', base, flags=re.IGNORECASE)
            stem = m.group(1) if m else f"page_{idx}"
            out_path = os.path.join(PROCESSED_DIR, f"{stem}.json")
            write_json(out_path, out)
            results.append({"source": base, "output": f"{stem}.json", "status": "success"})
            success += 1
        else:
            results.append({"source": os.path.basename(path), "output": None, "status": "failed", "error": err})
            failed += 1
        time.sleep(0.2)  # be gentle

    status = {
        "timestamp": utcnow_iso(),
        "files_found": len(html_files),
        "successful": success,
        "failed": failed,
        "results": results
    }
    write_json(os.path.join(STATUS_DIR, "process_complete.json"), status)
    print(f"[{utcnow_iso()}] Processor complete: {success} success, {failed} failed", flush=True)

if __name__ == "__main__":
    main()
