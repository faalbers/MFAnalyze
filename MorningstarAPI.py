import requests, json

# https://www.morningstar.com/api/v1/search/entities?q=VITAX&limit=1&autocomplete=false

def findQuote(symbol):
    url = 'https://www.morningstar.com/api/v1/search/entities?q=%s' % symbol
    result = requests.get(url=url)
    print(result.status_code)

if __name__ == "__main__":
    # findQuote('VITAX')
    # url = 'https://www.morningstar.com/'
    # url = 'https://www.morningstar.com/funds/xnas/vitax/quote'
    # https://api-global.morningstar.com/sal-service/v1/fund/process/asset/v2/FOUSA04BNE/data?languageId=en&locale=en&clientId=MDC&benchmarkId=mstarorcat&component=sal-components-mip-asset-allocation&version=3.74.0
    url = 'https://api-global.morningstar.com/sal-service/v1/fund/process/asset/v2/FOUSA04BNE/data?languageId=en&locale=en&clientId=MDC&benchmarkId=mstarorcat&component=sal-components-mip-asset-allocation&version=3.74.0'
    result = requests.get(url=url)

    print(result.status_code)

