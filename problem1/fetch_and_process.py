import sys
import os
import json
import time
import re
from urllib import request, error
import datetime
from datetime import datetime, timezone



def utc_now_iso_z():
    #hint by chatgpt
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")



def is_text_response(headers):
    content_type = headers.get("Content-Type", "")
    return "text" in content_type.lower()


def count_words(text):
    return len(re.findall(r"[A-Za-z0-9]+", text))


def fetch(url):
    start = time.perf_counter()
    result = {
        "url": url,
        "status_code": None,
        "response_time_ms": None,
        "content_length": 0,
        "word_count": None,
        "timestamp": now(),
        "error": None
    }
    try:
        with request.urlopen(url, timeout=10) as resp:
            body = resp.read()
            result["status_code"] = resp.status
            result["content_length"] = len(body)
            if is_text(resp.headers):
                text = body.decode(resp.headers.get_content_charset() or "utf-8", errors="ignore")
                result["word_count"] = count_words(text)
    except error.HTTPError as e:
        result["status_code"] = e.code
        result["error"] = str(e)
    except Exception as e:
        result["error"] = str(e)
    finally:
        result["response_time_ms"] = round((time.perf_counter() - start) * 1000, 2)
    return result


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    if len(sys.argv) != 3:
        print("Usage: python fetch_and_process.py <input_file> <output_dir>")
        sys.exit(1)

    infile, outdir = sys.argv[1], sys.argv[2]
    os.makedirs(outdir, exist_ok=True)

    with open(infile, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    start_time = now()
    results, errors = [], []
    for url in urls:
        r = fetch(url)
        results.append(r)
        if r["error"]:
            errors.append(f"[{r['timestamp']}] [{url}]: {r['error']}")

    with open(os.path.join(outdir, "responses.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    total = len(results)
    success = sum(1 for r in results if r["error"] is None)
    fail = total - success
    avg_time = round(sum(r["response_time_ms"] for r in results) / total, 2) if total else 0
    total_bytes = sum(r["content_length"] for r in results if not r["error"])
    status_dist = {}
    for r in results:
        if r["status_code"] is not None:
            code = str(r["status_code"])
            status_dist[code] = status_dist.get(code, 0) + 1

    summary = {
        "total_urls": total,
        "successful_requests": success,
        "failed_requests": fail,
        "average_response_time_ms": avg_time,
        "total_bytes_downloaded": total_bytes,
        "status_code_distribution": status_dist,
        "processing_start": start_time,
        "processing_end": now()
    }
    with open(os.path.join(outdir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(os.path.join(outdir, "errors.log"), "w", encoding="utf-8") as f:
        f.write("\n".join(errors))

if __name__ == "__main__":
    main()