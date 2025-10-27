import gzip, subprocess

def main():
    # Download wet.paths.gz
    subprocess.run(["wget", "-O", "wet.paths.gz", 
                    "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz"], check=True)
    # Read first URI
    with gzip.open("wet.paths.gz", "rt") as f:
        lines = f.readlines()
    first_uri = lines[0].strip()
    print(f"First WET URI: {first_uri}")
    
    # Download first WET file
    wet_url = "https://data.commoncrawl.org/" + first_uri
    subprocess.run(["wget", "-O", "wet_file.gz", wet_url], check=True)

    # Stream-print first 50 lines
    with gzip.open("wet_file.gz", "rt") as f:
        for i, line in enumerate(f):
            print(line.strip())

if __name__ == "__main__":
    main()
