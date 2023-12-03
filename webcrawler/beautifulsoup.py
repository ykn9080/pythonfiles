#!/bin/python3

from bs4 import BeautifulSoup

import httpx

# Send an HTTP GET request to the specified URL using the httpx library

response = httpx.get("https://news.ycombinator.com/news")

# Save the content of the response

yc_web_page = response.content

# Use the BeautifulSoup library to parse the HTML content of the webpage

soup = BeautifulSoup(yc_web_page)

# Find all elements with the class "athing" (which represent articles on Hacker News) using the parsed HTML

articles = soup.find_all(class_="athing")

# Loop through each article and extract relevant data, such as the URL, title, and rank

for article in articles:

    data = {

        # Find the URL of the article by finding the first "a" tag within the element with class "titleline"
        "URL": article.find(class_="titleline").find("a").get('href'),

        # Find the title of the article by getting the text content of the element with class "titleline"
        "title": article.find(class_="titleline").getText(),

        # Find the rank of the article by getting the text content of the element with class "rank" and removing the period character
        "rank": article.find(class_="rank").getText().replace(".", "")

    }

# Print the extracted data for the current article

print(data)
