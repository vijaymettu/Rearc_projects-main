#!/usr/bin/env python3
"""
Sync an HTTP directory (BLS time.series/pr) to an S3 prefix, keeping files
added/updated/deleted in sync without hard-coding filenames.

Features
- Discovers files dynamically from the HTTP index (no hard-coded names).
- Skips re-uploads using MD5/Last-Modified/Content-Length metadata stored on S3.
- Deletes S3 objects that no longer exist at the source (opt-in via --delete).
- Safe: only deletes objects that were previously uploaded by this script (tagged).
- Supports concurrent transfers.
- Retries with backoff for resilient network operations.

Requirements
- Python 3.9+
- boto3, botocore, requests, beautifulsoup4
"""

from __future__ import annotations
import argparse
import concurrent.futures as cf
import hashlib
import logging
import os
import sys
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urljoin

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import requests
from bs4 import BeautifulSoup

def md5_hexdigest_and_b64(data_iter, chunk_size=1024 * 1024) -> Tuple[str, str, int, bytes]:
    md5 = hashlib.md5()
    total = 0
    parts = []
    for chunk in data_iter:
        if not chunk:
            continue
        md5.update(chunk)
        total += len(chunk)
        parts.append(chunk)
    payload = b"".join(parts)
    import base64
    return md5.hexdigest(), base64.b64encode(md5.digest()).decode("utf-8"), total, payload

def requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "BLSDataSync/1.0 (vijayreddim@gmail.com)", 
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    })
    return s

def list_http_files(base_url: str, session: Optional[requests.Session] = None) -> Dict[str, Dict[str, str]]:
    session = session or requests_session()
    r = session.get(base_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = soup.find_all("a")
    files = {}
    for a in links:
        href = a.get("href")
        if not href or href in ("../", "./") or href.endswith("/"):
            continue
        name = href.split("/")[-1]
        file_url = urljoin(base_url, href)
        try:
            hr = session.head(file_url, timeout=60, allow_redirects=True)
            headers = hr.headers if hr.ok else {}
        except requests.RequestException:
            continue
        files[name] = {
            "url": file_url,
            "last_modified": headers.get("Last-Modified", ""),
            "content_length": headers.get("Content-Length", ""),
            "etag": headers.get("ETag", "").strip('"'),
        }
    return files

S3_TAG_SOURCE_KEY = "source"
S3_TAG_SOURCE_VAL = "bls"
S3_TAG_URLBASE_KEY = "source_url_base"

def get_s3_client(region: Optional[str]) -> boto3.client:
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"})
    return boto3.client("s3", region_name=region, config=cfg)

def head_s3_object(s3, bucket: str, key: str) -> Optional[dict]:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise

def get_s3_tags(s3, bucket: str, key: str) -> Dict[str, str]:
    try:
        resp = s3.get_object_tagging(Bucket=bucket, Key=key)
        return {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}
    except ClientError:
        return {}

def should_skip_upload(s3_head: Optional[dict], src_meta: Dict[str, str]) -> bool:
    if not s3_head:
        return False
    meta = {k.lower(): v for k, v in (s3_head.get("Metadata") or {}).items()}
    src_md5 = src_meta.get("md5", "")
    if src_md5 and meta.get("source_md5") == src_md5:
        return True
    src_len = src_meta.get("content_length", "")
    src_lm = src_meta.get("last_modified", "")
    if src_len and meta.get("source_length") == src_len and src_lm and meta.get("source_last_modified") == src_lm:
        return True
    src_etag = src_meta.get("etag", "")
    if src_etag and (meta.get("source_etag") == src_etag or (s3_head.get("ETag", "").strip('"') == src_etag)):
        return True
    return False

def upload_file(s3, bucket: str, key: str, content: bytes, src_meta: Dict[str, str]) -> None:
    metadata = {
        "source_md5": src_meta.get("md5", ""),
        "source_length": src_meta.get("content_length", ""),
        "source_last_modified": src_meta.get("last_modified", ""),
        "source_etag": src_meta.get("etag", ""),
        "source_url": src_meta.get("url", ""),
    }
    tags = {
        S3_TAG_SOURCE_KEY: S3_TAG_SOURCE_VAL,
        S3_TAG_URLBASE_KEY: src_meta.get("base_url", ""),
    }
    content_md5_b64 = src_meta.get("md5_b64", "")
    extra_args = {"Metadata": metadata, "Tagging": "&".join(f"{k}={v}" for k, v in tags.items())}
    if content_md5_b64:
        extra_args["ContentMD5"] = content_md5_b64
    s3.put_object(Bucket=bucket, Key=key, Body=content, **extra_args)

def delete_removed_objects(s3, bucket: str, prefix: str, present_names: set, base_url: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key[len(prefix):] if key.startswith(prefix) else key
            if name in present_names:
                continue
            tags = get_s3_tags(s3, bucket, key)
            if tags.get(S3_TAG_SOURCE_KEY) == S3_TAG_SOURCE_VAL and tags.get(S3_TAG_URLBASE_KEY) == base_url:
                logging.info("Deleting removed object: s3://%s/%s", bucket, key)
                s3.delete_object(Bucket=bucket, Key=key)

def backoff_sleep(try_idx: int):
    time.sleep(min(60, (2 ** try_idx)))

def download_with_retries(session: requests.Session, url: str, max_tries: int = 5, timeout: int = 120) -> requests.Response:
    last_exc = None
    for i in range(max_tries):
        try:
            r = session.get(url, stream=True, timeout=timeout)
            r.raise_for_status()
            return r
        except (requests.RequestException) as e:
            last_exc = e
            logging.warning("Download failed for %s (try %d/%d): %s", url, i + 1, max_tries, e)
            backoff_sleep(i)
    raise last_exc

def sync(base_url: str, bucket: str, prefix: str, region: Optional[str], concurrency: int, do_delete: bool) -> None:
    s3 = get_s3_client(region)
    session = requests_session()
    files = list_http_files(base_url, session=session)
    results = {"uploaded": [], "skipped": []}

    def process_one(name: str, info: Dict[str, str]):
        key = f"{prefix}{name}"
        s3_head = head_s3_object(s3, bucket, key)
        src_meta = {
            "url": info["url"],
            "last_modified": info.get("last_modified", ""),
            "content_length": info.get("content_length", ""),
            "etag": info.get("etag", ""),
            "base_url": base_url,
        }
        if should_skip_upload(s3_head, src_meta):
            return (name, "skipped")
        r = download_with_retries(session, info["url"])
        md5_hex, md5_b64, total, payload = md5_hexdigest_and_b64(r.iter_content(chunk_size=1024 * 1024))
        src_meta["md5"] = md5_hex
        src_meta["md5_b64"] = md5_b64
        src_meta["content_length"] = str(total)
        s3_head = head_s3_object(s3, bucket, key)
        if should_skip_upload(s3_head, src_meta):
            return (name, "skipped")
        upload_file(s3, bucket, key, payload, src_meta)
        return (name, "uploaded")

    with cf.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        futs = {ex.submit(process_one, n, i): n for n, i in files.items()}
        for fut in cf.as_completed(futs):
            name = futs[fut]
            try:
                fname, status = fut.result()
            except Exception as e:
                logging.error("Failed processing %s: %s", name, e)
                continue
            results[status].append(fname)

    if do_delete:
        delete_removed_objects(s3, bucket, prefix, present_names=set(files.keys()), base_url=base_url)

    logging.info("Uploaded: %d, Skipped: %d", len(results["uploaded"]), len(results["skipped"]))

def main():
    parser = argparse.ArgumentParser(description="Sync BLS HTTP directory to S3")
    parser.add_argument("--base-url", required=True, help="HTTP directory root")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument("--prefix", required=True, help="Destination S3 prefix")
    parser.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None, help="AWS region")
    parser.add_argument("--concurrency", type=int, default=8, help="Number of parallel downloads/uploads")
    parser.add_argument("--delete", action="store_true", help="Delete S3 objects that no longer exist at source")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")

    if args.prefix and not args.prefix.endswith("/"):
        args.prefix = args.prefix + "/"
    if not args.base_url.endswith("/"):
        args.base_url = args.base_url + "/"

    try:
        sync(args.base_url, args.bucket, args.prefix, args.region, args.concurrency, args.delete)
    except KeyboardInterrupt:
        sys.exit(130)
    except EndpointConnectionError as e:
        logging.error("S3 endpoint error: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
