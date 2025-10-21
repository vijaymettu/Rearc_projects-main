import os
import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
# BLS requires a User-Agent header to avoid 403 Forbidden errors.
#  & "C:\Users\vijay\AppData\Local\Programs\Python\Python313\python.exe" "C:\A-Rearc\Code_for_rearc\Rearc_projects-main\src\rearc\Part1-SourcingDatatsets\rearc_data_local_sync.py"--base-url "http://download.bls.gov/pub/time.series/pr/" --dest-dir "C:\A-Rearc\Code_for_rearc\py_projects-main\src\rearc\data\bls_data" --concurrency 6

HEADERS = {
    "User-Agent": "BLSDataSync/1.0 (vijayreddim@gmail.com)"
}
def list_remote_files(base_url):
    print(f"Listing remote files from {base_url}...")
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error accessing URL {base_url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href or href in ("../",):
            continue
        if href.endswith("/"):  # skip directories
            continue

        # keep only the actual filename (strip directory parts)
        filename = os.path.basename(href.strip("/"))
        if filename:
            file_url = urljoin(base_url, href)
            links.append((filename, file_url))

    print(links, "\n")
    print(f"Found {len(links)} remote files.")
    return links

def needs_download(file_url, local_path):
    try:
        head = requests.head(file_url, headers=HEADERS, timeout=30, allow_redirects=True)
        head.raise_for_status()
        remote_size = int(head.headers.get("Content-Length", -1))
    except (requests.exceptions.RequestException, ValueError):
        print(f"Could not get size for {file_url}. Attempting download.")
        return True

    if not os.path.exists(local_path):
        print(f"Local file not found: {local_path}. Will download.")
        return True

    local_size = os.path.getsize(local_path)
    if local_size != remote_size:
        print(f"Size mismatch for {local_path}. Local: {local_size} bytes, Remote: {remote_size} bytes. Will download.")
        return True

    print(f"Up to date, skipping: {local_path}")
    return False

def download_file(file_url, local_path):
    try:
        response = requests.get(file_url, headers=HEADERS, timeout=60, stream=True)
        response.raise_for_status()
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {local_path}")
    except Exception as e:
        print(f"Failed to download {file_url} to {local_path}: {e}")

def sync_files(base_url, dest_dir, concurrency=4, delete=False):
    remote_files = list_remote_files(base_url)
    remote_names = {name for name, _ in remote_files}
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {}
        for name, url in remote_files:
            local_path = os.path.join(dest_dir, name)
            if needs_download(url, local_path):
                futures[executor.submit(download_file, url, local_path)] = name
        for future in as_completed(futures):
            future.result()
    if delete:
        print("Checking for stale local files to delete...")
        for root, _, files in os.walk(dest_dir):
            for f in files:
                if f not in remote_names:
                    local_path = os.path.join(root, f)
                    try:
                        os.remove(local_path)
                        print(f"Deleted stale file: {local_path}")
                    except OSError as e:
                        print(f"Error deleting file {local_path}: {e}")
                        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync BLS files to local directory")
    parser.add_argument("--base-url", required=True, help="Base URL of BLS dataset")
    parser.add_argument("--dest-dir", required=True, help="Destination local directory")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of parallel downloads")
    parser.add_argument("--delete", action="store_true", help="Delete local files not in remote")
    args = parser.parse_args()
    sync_files(args.base_url, args.dest_dir, args.concurrency, args.delete)