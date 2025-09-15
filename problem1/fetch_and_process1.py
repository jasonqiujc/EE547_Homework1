#!/usr/bin/env python3
import sys, os, json, time, re
from datetime import datetime, timezone
from urllib import request, error

WORD_RE = re.compile(r"[A-Za-z0-9]+")
TEXT_CT_RE = re.compile(r"text", re.IGNORECASE)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def is_text_response(headers) -> bool:
    ct = headers.get("Content-Type", "")
    return bool(TEXT_CT_RE.search(ct))

def fetch_one(url: str, timeout=10):
    t0 = time.perf_counter()
    try:
        with request.urlopen(url, timeout=timeout) as resp:
            body = resp.read()
            dt_ms = (time.perf_counter() - t0) * 1000.0
            status = getattr(resp, "status", 200)
            headers = resp.headers
            content_len = len(body)
            word_count = None
            if is_text_response(headers):
                try:
                    text = body.decode(resp.headers.get_content_charset() or "utf-8", errors="ignore")
                except Exception:
                    text = body.decode("utf-8", errors="ignore")
                word_count = len(WORD_RE.findall(text))
            return {
                "url": url,
                "status_code": int(status),
                "response_time_ms": dt_ms,
                "content_length": content_len,
                "word_count": word_count,
                "timestamp": utc_now_iso(),
                "error": None,
            }
    except Exception as e:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        # 当请求失败时也记录时间，content_length/word_count 置合理默认
        return {
            "url": url,
            "status_code": None,
            "response_time_ms": dt_ms,
            "content_length": 0,
            "word_count": None,
            "timestamp": utc_now_iso(),
            "error": str(e),
        }

def main():
    if len(sys.argv) != 2 + 1:
        print("Usage: fetch_and_process.py <input_file> <output_dir>", file=sys.stderr)
        sys.exit(2)

    in_path, out_dir = sys.argv[1], sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)

    # 读 URL 列表
    with open(in_path, "r", encoding="utf-8") as f:
        urls = [ln.strip() for ln in f if ln.strip()]

    processing_start = utc_now_iso()
    results = []
    status_dist = {}
    total_bytes = 0
    ok, fail = 0, 0

    # 抓取
    for url in urls:
        rec = fetch_one(url, timeout=10)
        results.append(rec)
        if rec["error"] is None and isinstance(rec["status_code"], int):
            ok += 1
            total_bytes += rec["content_length"]
            status_dist[str(rec["status_code"])] = status_dist.get(str(rec["status_code"]), 0) + 1
        else:
            fail += 1

    processing_end = utc_now_iso()

    # write into responses.json
    with open(os.path.join(out_dir, "responses.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    # write into summary.json
    avg_rt = sum(r["response_time_ms"] for r in results) / len(results) if results else 0.0
    summary = {
        "total_urls": len(urls),
        "successful_requests": ok,
        "failed_requests": fail,
        "average_response_time_ms": avg_rt,
        "total_bytes_downloaded": total_bytes,
        "status_code_distribution": status_dist,
        "processing_start": processing_start,
        "processing_end": processing_end,
    }
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # 写 errors.log
    with open(os.path.join(out_dir, "errors.log"), "w", encoding="utf-8") as f:
        for r in results:
            if r["error"]:
                f.write(f"{r['timestamp']} {r['url']}: {r['error']}\n")

if __name__ == "__main__":
    main()
