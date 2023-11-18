import requests
from bs4 import BeautifulSoup

# The URL of the page to scrape
url = 'https://tradingeconomics.com/calendar'

# Send an HTTP request to the URL
response = requests.get(url)

# Check if the request was successful
if response.ok:
    # Parse the HTML content of the page with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all the times, which are in <span> elements with class 'calendar-date-3'
    times = [span.get_text() for span in soup.find_all('span', class_='calendar-date-3')]

    # Find all the countries, which are in <td> elements with class 'calendar-iso'
    countries = [td.get_text() for td in soup.find_all('td', class_='calendar-iso')]

    # The rest of the data is in <td> elements with class 'calendar-item calendar-item-positive'
    data = [td.get_text() for td in soup.find_all('td', class_='calendar-item calendar-item-positive')]

    # Since the data is structured in a table, we can assume every four items (Actual, Previous, Consensus, Forecast) belong to one entry
    structured_data = [data[i:i+4] for i in range(0, len(data), 4)]

    # Combine the extracted data
    combined_data = list(zip(times, countries, structured_data))

    # Print the combined data
    for entry in combined_data:
        time, country, data_entry = entry
        print(f"Time: {time}, Country: {country}, Data: {data_entry}")
else:
    print("Failed to retrieve the webpage")