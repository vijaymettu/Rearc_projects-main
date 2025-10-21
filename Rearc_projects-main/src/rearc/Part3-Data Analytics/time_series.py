import urllib3

http = urllib3.PoolManager()

url = "https://download.bls.gov/pub/time.series/pr/pr.data.0.Current"

response = http.request("GET", url)

if response.status == 200:
    with open("src/rearc/data/bls_data.csv", "wb") as f:
        f.write(response.data)
    print("File downloaded and saved as bls_data.csv")
else:
    print(f"Failed to download file: HTTP {response.status}")
