import requests, json
import DataScrape as DS

if __name__ == "__main__":
    url = 'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords=VITAX&apikey=5I186NCN3TF27H5P'
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=VITAX&apikey=5I186NCN3TF27H5P'
    result = requests.get(url=url)
    print(result.status_code)
    print(result.text)
    
