import DataScrape as DS
from bs4 import BeautifulSoup
from datetime import datetime
import logging, time
from multiprocessing.dummy import Pool
import yfinance as yf

# --- TOOLS ---

def attributeCheck(attribute):
    # checks = {'NetExpenseRatio': 'ExpenseRatio',
    # 'TotalExpenseRatio': 'ExpenseRatio'}
    checks = {}

    if attribute in checks: return checks[attribute]
    return attribute

def symbolsNeedScrape(MFData, dataName, seconds=0, minutes=0, hours=0, days=0):
    if not 'Symbols' in MFData: return []
    symbols = list(MFData['Symbols'])
    symbols.sort()
    if not dataName in MFData: return symbols
    minuteSecs = 60
    hourSecs = minuteSecs * 60
    daySecs = hourSecs * 24
    totalSecs = days * daySecs + hours * hourSecs + minutes * minuteSecs + seconds
    nowTime = datetime.now()
    symbolsDone = set()
    for symbol, data in MFData[dataName].items():
        diffSecs = (nowTime - data['ScrapeTag']).total_seconds()
        if diffSecs <= totalSecs:
            symbolsDone.add(symbol)
    symbols = list(MFData['Symbols'].difference(symbolsDone))
    symbols.sort()
    return symbols

def cleanUpAttributes(attributes, capitalize=True):
    data = {}
    for attr, value in attributes.items():
        if attr == 'Managers' or attr == 'Manager' :
            data[attr] = value.strip().split('\n')
        else:
            attrName = ''
            currencies = '$£₹€¥₩'
            mults = 'KMBTp'
            
            for attrPart in attr.split():
                if attrPart.startswith('('): continue
                elif attrPart.startswith('%'): continue
                if capitalize:
                    attrName = attrName + attrPart.replace('.','').capitalize()
                else:
                    attrName = attrName + attrPart.replace('.','')
            
            if value == '' or value == 'N/A':
                data[attrName] = None
                continue

            splitValue = value.split()
            firstValue = splitValue[0].replace(',','')
            if len(splitValue) == 2 and firstValue.replace('.','').isnumeric():
                data[attrName] = [float(firstValue), splitValue[1]]
                continue

            if value.endswith('%'):
                numtest = value.replace('%','')
                numtest = numtest.replace(',','')
                if numtest.replace('.','').replace('-','').isnumeric():
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
            if value.strip('-').replace('.','').replace(',','').isnumeric():
                value = float(value.replace(',',''))
                data[attrName] = value
                continue
            data[attrName] = value

    return data

def getMWtbodyData(tbodies):
    data = {}
    attributes = {}
    holdings = {}
    for tbody in tbodies:
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) == 2:
                subValues = tds[1].text.split('\n')
                if len(subValues) > 1:
                    attributes[tds[0].text] = subValues[2]
                else:
                    attributes[tds[0].text] = subValues[0]
            if len(tds) == 3:
                if tds[1].text == '-' or tds[1].text.startswith('$'):
                    continue
                holdings[tds[1].text] = tds[2].text

    for attr, value in cleanUpAttributes(attributes).items():
        data[attr] = value
    
    if len(holdings) > 0:
        data['Holdings'] = cleanUpAttributes(holdings, capitalize=False)
    
    return data

def getMWulData(uls):
    data = {}
    attributes = {}
    for ul in uls:
        for li in ul.find_all('li'):
            small = li.find('small')
            if small != None:
                attributes[small.text] = li.find('span', {'class': 'primary'}).text
    
    for attr, value in cleanUpAttributes(attributes).items():
        data[attr] = value

    return data

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

def mfQuoteDataMWSB4Proc(symbol):
    # logging.info('%s: scraping MarketWatch Data' % symbol)
    data = {}
    url = 'https://www.marketwatch.com/investing/fund/%s' % symbol
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, {}]

    # check url redirection for useful data or no data at all
    urlRedirect = r.url
    data['Type'] = None
    data['CountryCode'] = None
    data['ISO'] = None
    data['SymbolName'] = None
    if urlRedirect != url:
        tail = urlRedirect.replace('https://www.marketwatch.com/','')
        if tail.startswith('search'):
            # symbol was not found
            return [statusCode, {}]
        # attributes = {}
        subdata = tail.split('/')
        data['Types'] = subdata[1]
        subdata = subdata[2].split('?')
        data['SymbolName'] = subdata[0].upper()
        if len(subdata) != 1:
            subdata = subdata[1].split('&')
            for item in subdata:
                itemSplit = item.split('=')
                if itemSplit[0] == 'countryCode':
                    data['CountryCode'] = itemSplit[1]
                elif itemSplit[0] == 'iso':
                    data['ISO'] = itemSplit[1]
    else:
        data['Type'] = 'fund'
        data['SymbolName'] = symbol
    
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # primary info
    primary = soup.find('div', {'class': 'region region--primary'})
    tbodies = []
    uls = []
    if primary != None:
        cprimary = primary.find('div', {'class': 'column column--primary'})
        if cprimary != None:
            tbodies = tbodies + cprimary.find_all('tbody')
        for aside in primary.find_all('div', {'class': 'column column--aside'}):
            tbodies = tbodies + aside.find_all('tbody')
            uls = uls + aside.find_all('ul')
    
    for attr, value in getMWtbodyData(tbodies).items():
        data[attr] = value
    for attr, value in getMWulData(uls).items():
        data[attr] = value
    
    return [statusCode, data]

def mfQuoteDataYFSB4Proc(symbol):
    excludeList = ['DIGI']
    if symbol in excludeList:
        logging.info('excluding symbol: %s' % symbol)
        return [200, {}]
    
    data = {}
    url = 'https://finance.yahoo.com/quote/%s' % symbol
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, {}]
    
    # check url redirection for useful data or no data at all
    urlRedirect = r.url
    if urlRedirect != url:
        tail = urlRedirect.replace('https://finance.yahoo.com/','')
        subdata = tail.split('?')
        if subdata[0] == 'lookup':
            return [statusCode, {}]

    soup = BeautifulSoup(r.text, 'html.parser')

    divs = []
    divs += soup.find_all('div', {'data-test': 'left-summary-table'})
    divs += soup.find_all('div', {'data-test': 'right-summary-table'})
    tbodies = []
    for div in divs:
        tbodies += div.find_all('tbody')
    
    attributes = {}
    for tbody in tbodies:
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            attribute = tds[0].text.split(' (')[0]
            attributes[attribute] = tds[1].text
    
    for attr, value in cleanUpAttributes(attributes).items():
        data[attr] = value
    
    return [statusCode, data]

# def mfQuoteDataGFSB4Proc(symbolVar):
#     # https://www.google.com/finance/quote/SOGU:NASDAQ
#     data = {}
#     symbol = symbolVar[0]
#     exchange = symbolVar[1]
#     # XNYS: https://www.morningstar.com/cefs/xnys/PAXS/quote
#     # XNYS: https://www.morningstar.com/stocks/xnys/MVO/quote
#     # XASE: https://www.morningstar.com/cefs/xase/AEF/quote
#     # ARCX: https://www.morningstar.com/etfs/arcx/SPSM/quote
#     # BATS: https://www.morningstar.com/etfs/bats/STOTS/quote
#     # XNAS: https://www.morningstar.com/funds/xnas/VITAX/quote
#     # XNAS: https://www.morningstar.com/etfs/xnas/SOGU/quote
#     # XNAS: https://www.morningstar.com/stocks/xnas/HRZN/quote
#     # XETR: None LMWE
#     # OOTC: https://www.morningstar.com/stocks/pinx/AWRRF/quote
#     # OOTC: None ISMXF

#     if exchange == 'XNYS': exchange = 'NYSE'
#     elif exchange == 'XASE': exchange = 'NYSEAMERICAN'
#     elif exchange == 'ARCX': exchange = 'NYSEARCA'
#     elif exchange == 'XNAS': exchange = 'MUTF'
#     elif exchange == 'OOTC': exchange = 'OTCMKTS'
#     else:
#         logging.info()

#     url = 'https://www.google.com/finance/quote/' % (type+'S', exchange, symbol)
    
#     r = DS.getRequest(url)
#     statusCode = r.status_code

#     if statusCode != 200:
#         return [statusCode, data]

def mfQuoteInfoYF(symbol):
    ticker = yf.Ticker(symbol)
    return ticker.info

# --- MAIN SCRAPERS ---

def getMFQuotesMWBS4(dataFileName):
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchQuotes'
    if not 'Symbols' in MFData: MFData['Symbols'] = set()
    if not dataName in MFData: MFData['MarketWatchQuotes'] = {}

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
    for result in results[1]:
        if type(result) == dict:
            for symbol, quoteData in result.items():
                MFData['Symbols'].add(symbol)
                MFData[dataName][symbol] = {}
                for attr, value in quoteData.items():
                    MFData[dataName][symbol][attributeCheck(attr)] = value
                MFData[dataName][symbol]['ScrapeTag'] = datetime.now()
    logging.info('Total symbols in data: %s' % len(MFData['Symbols']))
    DS.saveData(MFData, dataFileName)

def getMFTypeMSBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'MorningStarTypes'
    if not 'Symbols' in MFData:
        logging.info('No symbols found in data !')
        return
    if not 'MarketWatchQuotes' in MFData:
        logging.info('Could not retrieve Exchange data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    symbols = symbolsNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)
    symbolExchanges = []
    for symbol in symbols:
        symbolExchanges.append([symbol, MFData['MarketWatchQuotes'][symbol]['Exchange']])

    sTotal = len(symbolExchanges)
    for block in DS.makeMultiBlocks(symbolExchanges, 300):
        logging.info('symbols to scrape types with MorningStar: %s' % sTotal)
        exchangeData = DS.multiScrape(block, mfTypeMSBS4Proc)

        sIndex = 0
        for data in exchangeData[1]:
            symbol = block[sIndex][0]
            if not symbol in MFData[dataName]: MFData[dataName][symbol] = {}
            MFData[dataName][symbol]['ScrapeTag'] = datetime.now()
            attribute = attributeCheck('Type')
            if data != None:
                data = data[:-1]
            MFData[dataName][symbol]['Type'] = data
            sIndex += 1
        
        DS.saveData(MFData, dataFileName)
        
        sTotal = sTotal - len(block)

def getMFQuoteDataMSSB4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'MorningStarQuoteData'
    if not 'Symbols' in MFData:
        logging.info('No symbols found in data !')
        return
    if not 'MarketWatchQuotes' in MFData:
        logging.info('Could not retrieve Exchange data !')
        return
    if not 'MorningStarTypes' in MFData:
        logging.info('Could not retrieve Type data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    typeSymbols = set()
    for symbol, data in MFData['MorningStarTypes'].items():
        if data['Type'] != None: typeSymbols.add(symbol)
    todoSymbols = symbolsNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)
    symbols = list(typeSymbols.intersection(set(todoSymbols)))
    symbols.sort()
    qDataSymbols = []
    for symbol in symbols:
        qDataSymbols.append([symbol, MFData['MarketWatchQuotes'][symbol]['Exchange'], MFData['MorningStarTypes'][symbol]['Type']])

    sTotal = len(qDataSymbols)
    for block in DS.makeMultiBlocks(qDataSymbols, 300):
        logging.info('symbols to scrape quote data with MorningStar: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfQuoteDataMSSB4Proc)

        sIndex = 0
        for data in quoteData[1]:
            symbol = block[sIndex][0]
            if not symbol in MFData[dataName]: MFData[dataName][symbol] = {}
            MFData[dataName][symbol]['ScrapeTag'] = datetime.now()
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                MFData[dataName][symbol][attribute] = value
            sIndex += 1
        
        DS.saveData(MFData, dataFileName)
        
        sTotal = sTotal - len(block)

def getMFQuoteDataMWSB4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchQuoteData'
    if not 'Symbols' in MFData:
        logging.info('No symbols found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoSymbols = symbolsNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    sTotal = len(todoSymbols)
    for block in DS.makeMultiBlocks(todoSymbols, 100):
        logging.info('symbols to scrape quote data with MarketWatch: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfQuoteDataMWSB4Proc, retryStatusCode=403)

        sIndex = 0
        for data in quoteData[1]:
            symbol = block[sIndex]
            if not symbol in MFData[dataName]: MFData[dataName][symbol] = {}
            MFData[dataName][symbol]['ScrapeTag'] = datetime.now()
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                MFData[dataName][symbol][attribute] = value
            sIndex += 1

        DS.saveData(MFData, dataFileName)
        
        sTotal = sTotal - len(block)

def getMFQuoteDataYFSB4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'YahooFinanceQuoteData'
    if not 'Symbols' in MFData:
        logging.info('No symbols found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoSymbols = symbolsNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    sTotal = len(todoSymbols)
    for block in DS.makeMultiBlocks(todoSymbols, 10):
        logging.info('symbols to scrape quote data with Yahoo Finance: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfQuoteDataYFSB4Proc, retryStatusCode=404)

        sIndex = 0
        for data in quoteData[1]:
            symbol = block[sIndex]
            if not symbol in MFData[dataName]: MFData[dataName][symbol] = {}
            MFData[dataName][symbol]['ScrapeTag'] = datetime.now()
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                MFData[dataName][symbol][attribute] = value
            sIndex += 1

        DS.saveData(MFData, dataFileName)
        
        sTotal = sTotal - len(block)

def getMFQuoteInfoYF(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'YFinanceTickerInfo'
    if not 'Symbols' in MFData:
        logging.info('No symbols found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoSymbols = symbolsNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    processes = 4
    sleepStepTime = 30
    multiPool = Pool(processes)
    sTotal = len(todoSymbols)
    for block in DS.makeMultiBlocks(todoSymbols, processes):
        logging.info('symbols to get quote info with YFinance: %s' % sTotal)
        results = []
        retry = True
        while retry:
            results = multiPool.map(mfQuoteInfoYF, block)
            retry = False

            # test if locked
            totalSleep = 0
            while len(mfQuoteInfoYF('VITAX')) < 4:
                retry = True
                time.sleep(sleepStepTime)
                totalSleep += sleepStepTime
                logging.info('waiting for %s seconds to unblock' % totalSleep)
            if retry:
                logging.info('retry same symbols to scrape quote data with Yahoo Finance')

        sIndex = 0
        for data in results:
            symbol = block[sIndex]
            if not symbol in MFData[dataName]: MFData[dataName][symbol] = {}
            MFData[dataName][symbol]['ScrapeTag'] = datetime.now()
            MFData[dataName][symbol]['Info'] = data
            sIndex += 1

        DS.saveData(MFData, dataFileName)
        
        sTotal = sTotal - len(block)

if __name__ == "__main__":
    scrapedFileName = 'MF_DATA_SCRAPED'
    DS.setupLogging('MFDataScrape.log', timed=True, new=False)
    logging.info('Start ...')

    # # create MFData by retrieving mutul funds from MarketWatch
    # getMFQuotesMWBS4(scrapedFileName)

    # # get investment type fom MorningStar links
    # # fast (1h 23 min for 38043 symbols) (29 min for 38040 symbols)
    # getMFTypeMSBS4(scrapedFileName, days=1)

    # # get quote data from MorningStar
    # # fast (1h 28 min for 38043 symbols) (34 min for 34560 symbols)
    # getMFQuoteDataMSSB4(scrapedFileName, days=1)

    # # get quote data from MarketWatch
    # # slow because of blocking
    # getMFQuoteDataMWSB4(scrapedFileName, days=1)

    # # get quote data from YahooFinance
    # # slow because of blocking
    # getMFQuoteDataYFSB4(scrapedFileName, days=1)

    # # get quote info from YahooFinance
    # # very slow because of blocking
    # getMFQuoteInfoYF(scrapedFileName, days=1)

    # creating data file
    dataFileName = 'MF_DATA'
    MFData = DS.getData(dataFileName)
    MFSData = DS.getData(scrapedFileName)

    for symbol, data in MFSData['MarketWatchQuotes'].items():
        MFData[symbol] = {}
        if data['Sector'] != '':
            MFData[symbol]['Sector'] = data['Sector']
        if data['Exchange'] != '':
            MFData[symbol]['Exchange'] = data['Exchange']
        MFData[symbol]['Name'] = data['Name']

    DS.saveData(MFData, dataFileName)
