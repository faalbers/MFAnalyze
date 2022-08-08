import requests, json
import DataScrape as DS

if __name__ == "__main__":
    dataFileName = 'FINAPI_DATA_MF'
    # cbmqfhiad3i7vqvd095g
    # 30 API calls/ second limit
    # If your limit is exceeded, you will receive a response with status code 429

    headers = {}
    params = {}

    headers['X-Finnhub-Token'] = 'cbmqfhiad3i7vqvd095g'

    # url = 'https://finnhub.io/api/v1/stock/symbol?exchange=US'
    url = 'https://finnhub.io/api/v1//mutual-fund/profile?symbol=VITAX'
    result = requests.get(url=url, params=params, headers=headers)
    print(result.status_code)
    # data = json.loads(result.text)
    print(result.text)

    # DS.saveData(data, dataFileName)

    # data = DS.getData(dataFileName)

    # types = set()
    # for quote in data:
    #     types.add(quote['type'])
    
    # print(types)
