import DataScrape as DS
from bs4 import BeautifulSoup
from datetime import datetime
import time, logging

# --- TOOLS ---

def cleanUpAttributes(attributes, capitalize=True):
    data = {}
    for attr, value in attributes.items():
        if value == 'N/A': continue            
        elif attr == 'Managers' or attr == 'Manager' :
            data[attr] = value.strip().split('\n')
        else:
            attrName = ''
            currencies = '$£₹€¥'
            mults = 'KMBTp'
            
            for attrPart in attr.split():
                if attrPart.startswith('('): continue
                elif attrPart.startswith('%'): continue
                if capitalize:
                    attrName = attrName + attrPart.replace('.','').capitalize()
                else:
                    attrName = attrName + attrPart.replace('.','')
            
            splitValue = value.split()
            firstValue = splitValue[0].replace(',','')
            if len(splitValue) == 2 and firstValue.replace('.','').isnumeric():
                data[attrName] = [float(firstValue), splitValue[1]]
                continue

            if value.endswith('%'):
                numtest = value.replace('%','')
                numtest = numtest.replace(',','')
                if numtest.replace('.','').isnumeric():
                    value = [float(numtest), '%']
                    data[attrName] = value
                    continue
            
            if len(value) > 0 and value[0] in currencies:
                unit = value[0]
                numtest = value.replace(value[0],'')
                if len(numtest) > 0 and numtest[-1] in mults:
                    mult = numtest[-1]
                    numtest = numtest.replace(mult,'')
                    unit = mult + unit
                numtest = numtest.replace(',','')
                if numtest.replace('.','').isnumeric():
                    value = [float(numtest), unit]
                    data[attrName] = value
                    continue
            if len(value) > 0 and value[-1] in mults:
                mult = value[-1]
                numtest = value.replace(mult,'')
                numtest = numtest.replace(',','')
                if numtest.replace('.','').isnumeric():
                    value = [float(numtest), mult]
                    data[attrName] = value
                    continue
            if value.replace('.','').replace(',','').replace('-','').isnumeric():
                value = float(value.replace(',',''))
                data[attrName] = value
                continue
            data[attrName] = value
    
    return data

# def getMWtbodyData(tbodies):
#     data = {}
#     attributes = {}
#     holdings = {}
#     for tbody in tbodies:
#         for tr in tbody.find_all('tr'):
#             tds = tr.find_all('td')
#             if len(tds) == 2:
#                 subValues = tds[1].text.split('\n')
#                 if len(subValues) > 1:
#                     attributes[tds[0].text] = subValues[2]
#                 else:
#                     attributes[tds[0].text] = subValues[0]
#             if len(tds) == 3:
#                 if tds[1].text == '-' or tds[1].text.startswith('$'):
#                     continue
#                 holdings[tds[1].text] = tds[2].text

#     for attr, value in cleanUpAttributes(attributes).items():
#         data[attr] = value
    
#     if len(holdings) > 0:
#         data['Holdings'] = cleanUpAttributes(holdings, capitalize=False)

#     return data

# def getMWulData(uls):
#     data = {}
#     attributes = {}
#     for ul in uls:
#         for li in ul.find_all('li'):
#             small = li.find('small')
#             if small != None:
#                 attributes[small.text] = li.find('span', {'class': 'primary'}).text
    
#     for attr, value in cleanUpAttributes(attributes).items():
#         data[attr] = value

#     return data

def attributeCheck(attribute):
    checks = {'NetExpenseRatio': 'ExpenseRatio',
    'TotalExpenseRatio': 'ExpenseRatio'}
    if attribute in checks: return checks[attribute]
    return attribute

# def getScrapeTime(dataFileName, scrapeTag):
#     MFData = DS.getData(dataFileName)

#     timeStamps = []
#     for symbol, data in MFData.items():
#         timeStamps.append(data['ScrapeTags'][scrapeTag])
#     timeStamps.sort()

#     if len(timeStamps) > 0:
#         return timeStamps[0]
#     return None

# def getScrapeTimeSince(dataFileName, scrapeTag):
#     return (datetime.now() - getScrapeTime(dataFileName, scrapeTag))

def symbolsNeedScrape(dataFileName, scrapeTag, seconds=0, minutes=0, hours=0, days=0):
    minuteSecs = 60
    hourSecs = minuteSecs * 60
    daySecs = hourSecs * 24
    totalSecs = days * daySecs + hours * hourSecs + minutes * minuteSecs + seconds
    nowTime = datetime.now()
    MFData = DS.getData(dataFileName)
    symbols = []
    for symbol, data in MFData.items():
        if scrapeTag in data['ScrapeTags']:
            diffSecs = (nowTime - data['ScrapeTags'][scrapeTag]).total_seconds()
            if diffSecs > totalSecs:
                symbols.append(symbol)
        else:
            symbols.append(symbol)
    symbols.sort()
    return symbols

def symbolsWithScrape(dataFileName, scrapeTag):
    MFData = DS.getData(dataFileName)
    symbols = []
    for symbol, data in MFData.items():
        if scrapeTag in data['ScrapeTags']:
            if scrapeTag in data['ScrapeTags']:
                symbols.append(symbol)
    symbols.sort()
    return symbols

# --- MULTI PROCS ---

def marketWatchPagesProc(letter):
    # logging.info('scraping MarketWatch pages from letter: %s' % letter)
    pages = None

    url = 'https://www.marketwatch.com/tools/markets/funds/a-z/%s/1000' % letter
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, pages]

    soup = BeautifulSoup(r.text, 'html.parser')

    paginations = soup.find_all('ul', class_='pagination')

    if len(paginations) == 1: return [statusCode, 1]

    pages = int(paginations[1].find_all('a')[-1].text.split('-')[-1])

    return [statusCode, pages]

def marketWatchQuotesProc(url):
    # logging.info('scraping MarketWatch quotes from: %s' % url)
    data = {}
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, data]

    soup = BeautifulSoup(r.text, 'html.parser')

    tbody = soup.find('tbody')
    trs = tbody.find_all('tr')
    for tr in trs:
        tds = tr.find_all('td')
        symbol = tds[0].a.small.text[1:-1]
        data[symbol] = {}
        data[symbol]['Name'] = tds[0].a.text.split(';')[0]
        data[symbol]['Country'] = tds[1].text
        data[symbol]['Exchange'] = tds[2].text
        if len(tds) > 3: data[symbol]['Sector'] = tds[3].text

    return [statusCode, data]

def mfTypeMSBS4Proc(symbolVar):
    # logging.info('%s: scraping MorninStar for type in exchange: %s' % (symbolVar[0], symbolVar[1]))
    msTypes = ['FUNDS', 'CEFS', 'ETFS', 'STOCKS']
    scode = None
    for msType in msTypes:
        url = 'https://www.morningstar.com/%s/%s/%s/quote' % (msType, symbolVar[1], symbolVar[0])
        scode = DS.getStatusCode(url)
        if scode == 200:
            return [scode, msType]
        elif scode != 404:
            if scode == 403:
                logging.info('blocked: %s' % url)
            elif scode == 500:
                logging.info('error: %s' % url)
            return [scode, None]
    return [scode, None]

def mfQuoteDataMSSB4Proc(symbolVar):
    data = {}
    symbol = symbolVar[0]
    exchange = symbolVar[1]
    type = symbolVar[2]
    # logging.info('%s: scraping MorningStar Data using exchange: %s and type:' % (exchange, type))

    # XNYS: https://www.morningstar.com/cefs/xnys/PAXS/quote
    # XNYS: https://www.morningstar.com/stocks/xnys/MVO/quote
    # XASE: https://www.morningstar.com/cefs/xase/AEF/quote
    # ARCX: https://www.morningstar.com/etfs/arcx/SPSM/quote
    # BATS: https://www.morningstar.com/etfs/bats/STOTS/quote
    # XNAS: https://www.morningstar.com/funds/xnas/VITAX/quote
    # XNAS: https://www.morningstar.com/etfs/xnas/SOGU/quote
    # XNAS: https://www.morningstar.com/stocks/xnas/HRZN/quote
    # XETR: None LMWE
    # OOTC: https://www.morningstar.com/stocks/pinx/AWRRF/quote
    # OOTC: None ISMXF

    url = 'https://www.morningstar.com/%s/%s/%s/quote' % (type+'S', exchange, symbol)
    
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, data]
    
    soup = BeautifulSoup(r.text, 'html.parser')

    morningStars = soup.find('span', class_='mdc-security-header__star-rating')
    if morningStars != None:
        data['MorningStars'] = int(morningStars['title'].split()[0])

    if type == 'FUND':
        content = soup.find('div', {'class': 'fund-quote__data'})
        if content == None: return [statusCode, data]
        for item in content.find_all('div', {'class': 'fund-quote__item'}):
            # get attribute names
            label = item.find('div', {'class': 'fund-quote__label'})
            attributes = label.text.split('\n')
            if len(attributes) > 1:
                attributes = attributes[1]
            else:
                attributes = attributes[0]
            attributes = attributes.replace('\t','')
            attributes = attributes.split(' / ')

            # get values
            value = item.find('div', {'class': 'fund-quote__value'})
            values = []
            if value != None:
                for span in value.find_all('span'):
                    spanValues = []
                    for spanValue in span.text.split('\n'):
                        spanValues.append(spanValue.replace('\t',''))
                    values.append(''.join(spanValues))
            else:
                values.append(item.find_all('span')[-1].text)

            # add to data
            aIndex = 0
            cleanupAttributes = {}
            for attribute in attributes:
                if attribute == 'Investment Style':
                    attrSplit = attribute.split()
                    valueSplit = values[aIndex].split()
                    data['Style'] = {}
                    data['Style'][attrSplit[0]] = valueSplit[0]
                    if len(valueSplit) >= 2:
                        data['Style'][attrSplit[1]] = valueSplit[1]
                    else:
                        data['Style'][attrSplit[1]] = valueSplit[-1]
                else:
                    if aIndex <= (len(values)-1):
                        cleanupAttributes[attribute] = values[aIndex]
                    else:
                        cleanupAttributes[attribute] = values[-1]
                aIndex += 1
            for attr, value in cleanUpAttributes(cleanupAttributes).items():
                data[attr] = value

    elif type == 'ETF':
        content = soup.find('div', {'class': 'etf__content'})
        # data unaccessible with BS4
    elif type == 'CEF':
        content = soup.find('div', {'class': 'cef__content'})
        # data unaccessible with BS4
    elif type == 'STOCK':
        content = soup.find('div', {'class': 'stock__content'})
        # data unaccessible with BS4
    else:
        logging.info('%s: No recognizable type for MorningStar: %s' % (symbol, type))

    return [statusCode, data]

# def quoteDataMarketWatchProc(symbol):
#     # logging.info('%s: scraping MarketWatch Data' % symbol)
#     data = { 'MWScraped': False }
    
#     url = 'https://www.marketwatch.com/investing/fund/%s' % symbol
#     r = DS.getRequest(url)
#     statusCode = r.status_code

#     if statusCode != 200:
#         return [statusCode, data]
    
#     soup = BeautifulSoup(r.text, 'html.parser')
    
#     # primary info
#     primary = soup.find('div', {'class': 'region region--primary'})
#     tbodies = []
#     uls = []
#     if primary != None:
#         cprimary = primary.find('div', {'class': 'column column--primary'})
#         if cprimary != None:
#             tbodies = tbodies + cprimary.find_all('tbody')
#         for aside in primary.find_all('div', {'class': 'column column--aside'}):
#             tbodies = tbodies + aside.find_all('tbody')
#             uls = uls + aside.find_all('ul')
    
#     for attr, value in getMWtbodyData(tbodies).items():
#         data[attr] = value
#     for attr, value in getMWulData(uls).items():
#         data[attr] = value
    
#     data['MWScraped'] = True

#     return [statusCode, data]

# --- MAIN SCRAPERS ---

def getMFQuotesMWBS4(dataFileName):
    MFData = DS.getData(dataFileName)
    scrapeTag = 'MW_Q'

    # scrape for alphabetic list pages
    letters = [chr(x) for x in range(65, 91)]
    results = DS.multiScrape(letters, marketWatchPagesProc)
    
    # get page url links
    lIndex = 0
    urls = []
    for letter in letters:
        pages = results[1][lIndex]
        if pages != None:
            for x in range(pages):
                urls.append('https://www.marketwatch.com/tools/markets/funds/a-z/%s/%s' % (letter, (x+1)))
        lIndex = lIndex + 1

    # get quotes from MarketWatch pages
    results = DS.multiScrape(urls, marketWatchQuotesProc)
    addCount = 0
    updateCount = 0
    for result in results[1]:
        if type(result) == dict:
            for symbol, quoteData in result.items():
                if not symbol in MFData:
                    MFData[symbol] = {'ScrapeTags': {}}
                    addCount = addCount + 1
                else:
                    updateCount = updateCount + 1
                for attr, value in quoteData.items():
                    MFData[symbol][attributeCheck(attr)] = value
                MFData[symbol]['ScrapeTags'][scrapeTag] = datetime.now()

    logging.info('MarketWatch Mutual Fund quotes found: %s' % (addCount + updateCount))
    logging.info('symbols added to data               : %s' % addCount)
    logging.info('symbols updated in data             : %s' % updateCount)

    DS.saveData(MFData, dataFileName)
    logging.info('total symbols in data               : %s' % len(MFData))

def getMFTypeMSBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    scrapeTag = 'MS_T'

    symbols = symbolsNeedScrape(dataFileName, scrapeTag, seconds=seconds, minutes=minutes, hours=hours, days=days)
    symbolExchanges = []
    for symbol in symbols:
        symbolExchanges.append([symbol, MFData[symbol]['Exchange']])

    sTotal = len(symbolExchanges)
    for block in DS.makeMultiBlocks(symbolExchanges, 300):
        logging.info('exchanges to check: %s' % sTotal)
        exchangeData = DS.multiScrape(block, mfTypeMSBS4Proc)

        sIndex = 0
        for data in exchangeData[1]:
            symbol = block[sIndex][0]
            if data != None: data = data[:-1]
            MFData[symbol][attributeCheck('Type')] = data
            MFData[symbol]['ScrapeTags'][scrapeTag] = datetime.now()
            sIndex += 1
        
        DS.saveData(MFData, dataFileName)
        
        sTotal = sTotal - len(block)

def getMFQuoteDataMSSB4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    scrapeTag = 'MS_QD'

    typeSymbols = symbolsWithScrape(dataFileName, 'MS_T')
    todoSymbols = symbolsNeedScrape(dataFileName, scrapeTag, seconds=seconds, minutes=minutes, hours=hours, days=days)
    symbols = list(set(typeSymbols).intersection(set(todoSymbols)))
    symbols.sort()
    qDataSymbols = []
    for symbol in symbols:
        if MFData[symbol]['Type'] == None: continue
        qDataSymbols.append([symbol, MFData[symbol]['Exchange'], MFData[symbol]['Type']])

    sTotal = len(qDataSymbols)
    for block in DS.makeMultiBlocks(qDataSymbols, 300):
        logging.info('symbols to scrape with MorningStar: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfQuoteDataMSSB4Proc)

        sIndex = 0
        for data in quoteData[1]:
            symbol = block[sIndex][0]
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                # if attribute in MFData[symbol]:
                #     before = MFData[symbol][attribute]
                #     logging.info('%s: Overwrite attribute: %s : %s -> %s' % (symbol, attribute, before, value))
                MFData[symbol][attribute] = value
            MFData[symbol]['ScrapeTags'][scrapeTag] = datetime.now()
            sIndex += 1
        
        DS.saveData(MFData, dataFileName)
        
        sTotal = sTotal - len(block)

# main program
if __name__ == "__main__":
    # start up
    dataFileName = 'MF_DATA'
    DS.setLogger('MFDataScrape.log', timed=True, new=False)
    logging.info('Start ...')

    getMFQuotesMWBS4(dataFileName)

    getMFTypeMSBS4(dataFileName, days=1)

    getMFQuoteDataMSSB4(dataFileName, days=1)

    # MFData = DS.getData(dataFileName)
    # for symbol, data in MFData.items():
    #     if 'CreditQuality'in data:
    #         print('CreditQuality: %s' % data['CreditQuality'])
    #     if 'InterestRateSensitivity'in data:
    #         print('InterestRateSensitivity: %s' % data['InterestRateSensitivity'])

    # # get MorningStar quote data
    # symbols = []
    # for symbol, data in MFData.items():
    #     if 'MSScraped' in data and data['MSScraped']: continue
    #     if 'Type' in data and data['Type'] != None:
    #         if data['Type'] == 'ETF':
    #             symbols.append([symbol, data['Exchange'], data['Type']])

    # sTotal = len(symbols)
    # for block in DS.makeMultiBlocks(symbols, 300):
    #     logging.info('symbols to scrape with MorningStar: %s' % sTotal)
    #     quoteData = DS.multiScrape(block, mfQuoteDataMSSB4Proc)

    #     for message in quoteData[2]:
    #         logging.info(message)

    #     sIndex = 0
    #     for data in quoteData[1]:
    #         symbol = block[sIndex][0]
    #         for attr, value in data.items():
    #             attribute = attributeCheck(attr)
    #             if attribute in MFData[symbol]:
    #                 before = MFData[symbol][attribute]
    #                 logging.info('%s: Overwrite attribute: %s : %s -> %s' % (symbol, attribute, before, value))
    #             MFData[symbol][attribute] = value
    #         sIndex += 1

    #     # DS.saveData(MFData, MFDataFileName)
        
    #     sTotal = sTotal - len(block)
    #     break

    # # get additional quotes data from MarketWatch
    # symbols = []
    # for symbol, data in MFData.items():
    #     if 'MWScraped' in data and data['MWScraped']: continue
    #     symbols.append(symbol)
    # symbols.sort()

    # sTotal = len(symbols)
    # for symbolsBlock in DS.makeMultiBlocks(symbols, 100):
    #     logging.info('symbols to scrape with MarketWatch: %s' % sTotal)
    #     quoteData = DS.multiScrape(symbolsBlock, quoteDataMarketWatchProc)

    #     for message in quoteData[2]:
    #         logging.info(message)

    #     sIndex = 0
    #     for data in quoteData[1]:
    #         for attr, value in data.items():
    #             MFData[symbolsBlock[sIndex]][attributeCheck(attr)] = value
    #         sIndex = sIndex + 1

    #     DS.saveData(MFData, MFDataFileName)
        
    #     sTotal = sTotal - len(symbolsBlock)

    # # some tests
    # quotes = ['VITAX']
    # USA searches
    # XNYS: https://www.morningstar.com/cefs/xnys/PAXS/quote
    # XASE: https://www.morningstar.com/cefs/xase/AEF/quote
    # ARCX: https://www.morningstar.com/etfs/arcx/SPSM/quote
    # BATS: https://www.morningstar.com/etfs/bats/STOTS/quote
    # XNAS: https://www.morningstar.com/funds/xnas/VITAX/quote
    # XETR: None LMWE
    # OOTC: None ISMXF
    
    # symbols = []
    # for symbol, data in MFData.items():
    #     if data['Exchange'] == 'BATS':
    #         symbols.append(symbol)
    # print(symbols)

    # symbols = ['VITAX', 'PAXS', 'AEF', 'SPSM', 'IBML', 'LMWE', 'ISMXF']
    # for symbol in symbols[0:1]:
    #     print(symbol)
    #     data = mfQuoteDataMSSB4Proc(symbol, MFData[symbol]['Exchange'])
    #     print(data)
    
    # for symbol in symbols:
    #     print(symbol)
    #     print(MFData[symbol]['Sector'])

    # sectors = set()
    # types = ['Exchange-Traded Funds', 'Closed-End Funds']
    # for symbol, data in MFData.items():
    #     if data['Exchange'] == 'XNAS':
    #         if data['Sector'] == 'Closed-End Funds':
    #             print(symbol)
    #         sectors.add(data['Sector'])
    # print(sectors)

    # # get quotes data from MorningStar
    # symbols = []
    # for symbol, data in MFData.items():
    #     if 'MSScraped' in data and data['MSScraped']: continue
    #     symbols.append(symbol)
    # symbols.sort()
 
    # sTotal = len(symbols)
    # for symbolsBlock in DS.makeMultiBlocks(symbols, 300):
    #     logging.info('symbols to scrape by MorningStar: %s' % sTotal)


    #     # quoteData = DS.multiScrape(symbolsBlock, quoteDataMarketWatchProc)

    #     # for message in quoteData[2]:
    #     #     logging.info(message)

    #     # sIndex = 0
    #     # for data in quoteData[1]:
    #     #     for attr, value in data.items():
    #     #         MFData[symbolsBlock[sIndex]][attributeCheck(attr)] = value
    #     #     sIndex += 1

    #     # DS.saveData(MFData, MFDataFileName)
        
    #     sTotal = sTotal - len(symbolsBlock)
    #     break