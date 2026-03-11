from gdelt_api import fetch_articles

def main():
    print("Global News Monitor starting...")

    data = fetch_articles()

    articles = data.get("articles", [])

    for article in articles[:5]:
        print(article["title"], "-", article["sourcecountry"])

if __name__ == "__main__":
    main()