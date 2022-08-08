import DataScrape as DS

dataFileName = 'MF_DATA'
CSVFileName = 'MF_DATA.csv'
MFData = DS.getData(dataFileName)

out = ''
out += 'Symbol, Name, CountryCode, Country,'
out += 'MorningStarRating, ExpenseRatio, MinInvestment, ETrade, Yield %,'
out += 'Stocks %, Cap, Style,'
out += 'Bonds %, CreditQuality, InterestRateSensitivity'
out += '\n'

for quote, data in MFData.items():
    quoteSplit = quote.split(':')
    symbol = quoteSplit[0]
    exchange = quoteSplit[1]
    countryCode = quoteSplit[2]
    name = data['Name'].replace(',','')
    
    msrating = 'N/A'
    if data['MorningStarRating'] != None: msrating = data['MorningStarRating']

    Yield = 'N/A'
    if data['Yield'] != None: Yield = data['Yield']

    expense = 'N/A'
    if data['ExpenseRatio'] != None: expense = data['ExpenseRatio']

    mininvest = 'N/A'
    if data['MinInvestment'] != None: mininvest = data['MinInvestment']

    stocks = 'N/A'
    cap = 'N/A'
    style = 'N/A'
    bonds = 'N/A'
    cquality = 'N/A'
    irsensitivity = 'N/A'

    if data['Allocations'] != None:
        stocks = data['Allocations']['Stocks']
        bonds = data['Allocations']['Bonds']

    if data['StockStyle'] != None:
        cap = data['StockStyle']['Cap']
        style = data['StockStyle']['Style']
    
    if data['BondStyle'] != None:
        cquality = data['BondStyle']['CreditQuality']
        irsensitivity = data['BondStyle']['InterestRateSensitivity']
    
    # out filters
    # if countryCode != 'US': continue
    # if msrating == 'N/A': continue

    out += '%s,%s,%s,%s,' % (symbol, name, countryCode, data['Country'])
    out += '%s,%s,%s,%s,%s,' % (msrating, expense, mininvest, data['ETradeAvailable'], Yield)
    out += '%s,%s,%s,' % (stocks, cap, style)
    out += '%s,%s,%s' % (bonds, cquality, irsensitivity)
    out += '\n'

with open(CSVFileName, 'w') as f:
    f.write(out)
