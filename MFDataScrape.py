import DataScrape as DS
import ETradeAPI as ET
from bs4 import BeautifulSoup
from datetime import datetime
import logging, time
from multiprocessing.dummy import Pool
import yfinance as yf

# --- TOOLS ---

def similarity(nameA, nameB):
    nameA = nameA.upper()
    nameB = nameB.upper()

    minLen = min(len(nameA), len(nameB))
    sameCount = 0
    for i in range(minLen):
        if nameA[i] != nameB[i]: break
        sameCount += 1
    
    return float(sameCount) / float(minLen)

def attributeCheck(attribute):
    # checks = {'NetExpenseRatio': 'ExpenseRatio',
    # 'TotalExpenseRatio': 'ExpenseRatio'}
    checks = {}

    if attribute in checks: return checks[attribute]
    return attribute

def quotesNeedScrape(MFData, dataName, needScrape=True, seconds=0, minutes=0, hours=0, days=0):
    if not dataName in MFData: return []
    minuteSecs = 60
    hourSecs = minuteSecs * 60
    daySecs = hourSecs * 24
    totalSecs = days * daySecs + hours * hourSecs + minutes * minuteSecs + seconds
    nowTime = datetime.now()
    quotesDone = set()
    for quote, data in MFData[dataName].items():
        diffSecs = (nowTime - data['ScrapeTag']).total_seconds()
        if diffSecs <= totalSecs:
            quotesDone.add(quote)
    quotes = set(MFData[dataName].keys())
    if needScrape:
        quotes = list(quotes.difference(quotesDone))
    else:
        quotes = list(quotesDone)
    return quotes

def cleanUpAttributes(attributes, capitalize=True, upper=False):
    data = {}
    for attr, value in attributes.items():
        attrName = ''
        currencies = '$£₹€¥₩'
        mults = 'KMBTp'
        
        for attrPart in attr.split():
            if attrPart.startswith('('): continue
            elif attrPart.startswith('%'): continue
            attrPart = attrPart.replace('.','')
            if upper:
                attrName = attrName + attrPart.upper()
            elif capitalize:
                attrName = attrName + attrPart.capitalize()
            else:
                attrName = attrName + attrPart
            
        if value == '' or value == 'N/A' or value == '-':
            data[attrName] = None
            continue

        values = [value]
        if type(value) == list:
            values = value
        cleanedValues = []
        for val in values:
            if '★' in val:
                cleanedValues.append(len(val))
                continue
            
            splitValue = val.split()
            firstValue = splitValue[0].replace(',','')
            if len(splitValue) == 2 and firstValue.replace('.','').isnumeric():
                cleanedValues.append([float(firstValue), splitValue[1]])
                continue

            if val.endswith('%'):
                numtest = val.replace('%','')
                numtest = numtest.replace(',','')
                if numtest.replace('.','').replace('-','').isnumeric():
                    val = [float(numtest), '%']
                    cleanedValues.append(val)
                    continue
            
            if len(val) > 0 and val[0] in currencies:
                unit = val[0]
                numtest = val.replace(val[0],'')
                if len(numtest) > 0 and numtest[-1] in mults:
                    mult = numtest[-1]
                    numtest = numtest.replace(mult,'')
                    unit = mult + unit
                numtest = numtest.replace(',','')
                if numtest.replace('.','').isnumeric():
                    val = [float(numtest), unit]
                    cleanedValues.append(val)
                    continue
            
            if len(val) > 0 and val[-1] in mults:
                mult = val[-1]
                numtest = val.replace(mult,'')
                numtest = numtest.replace(',','')
                if numtest.replace('.','').isnumeric():
                    val = [float(numtest), mult]
                    cleanedValues.append(val)
                    continue
            
            if val.strip('-').replace('.','').replace(',','').isnumeric():
                val = float(val.replace(',',''))
                cleanedValues.append(val)
                continue

            cleanedValues.append(val)

        if len(cleanedValues) > 1:
            data[attrName] = cleanedValues
        elif len(cleanedValues) == 0:
            data[attrName] = None
        else:
            data[attrName] = cleanedValues[0]

    return data

# --- MULTI PROCS ---

def marketWatchPagesProc(letter):
    # logging.info('scraping MarketWatch pages from letter: %s' % letter)
    pages = None

    url = 'https://www.marketwatch.com/tools/markets/funds/a-z/%s/1000' % letter
    r = DS.getRequest(url)
    if r == None:
        statusCode = 500
    else:
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
    data = []
    r = DS.getRequest(url)
    if r == None:
        statusCode = 500
    else:
        statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, data]

    soup = BeautifulSoup(r.text, 'html.parser')

    tbody = soup.find('tbody')
    trs = tbody.find_all('tr')
    for tr in trs:
        tds = tr.find_all('td')
        symbol = tds[0].a.small.text[1:-1]
        name = tds[0].a.text.split(';')[0]
        country = tds[1].text
        mic = tds[2].text
        sector = None
        if len(tds) > 3:
            sector = tds[3].text
        data.append('%s:%s:%s:%s:%s' % (symbol, mic, country, name, sector))

    return [statusCode, data]

def mfQuoteSearchOtherMSBS4Proc(quote):
    data = {}
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]

    url = 'https://www.morningstar.com/search?query=%s' % symbol

    r = DS.getRequest(url)
    if r == None:
        statusCode = 500
    else:
        statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, data]
    
    soup = BeautifulSoup(r.text, 'html.parser')

    empty = soup.find('div', class_='search-all__empty')
    if empty != None:
        return [statusCode, data]

    sections = soup.find_all('section', class_='search-all__section')
    for section in sections:
        sectionName = section.text.strip().split('\n')[0]
        if sectionName == 'U.S. Securities' or sectionName == 'Foreign Securities':
            for div in section.find_all('div', class_='mdc-security-module search-all__hit'):
                splits = div.text.split()
                foundSymbol = splits[-1]
                if foundSymbol != symbol: continue
                foundExchange = splits[-2]
                foundQuote = '%s:%s' % (foundSymbol, foundExchange)
                if foundQuote == quote: continue
                foundName = ' '.join(splits[:-2])
                data[foundQuote] = foundName
    return [statusCode, data]

def mfQuoteDataMSBS4Proc(quote):
    data = {}
    splits = quote.split(':')
    symbol = splits[0]
    exchange = splits[1]
    mfTypes = ['FUNDS', 'CEFS', 'ETFS']
    statusCode = 404
    r = None
    fundType = None
    for mfType in mfTypes:
        url = 'https://www.morningstar.com/%s/%s/%s/quote' % (mfType, exchange, symbol)
        r = DS.getRequest(url)
        if r == None:
            statusCode = 500
        else:
            statusCode = r.status_code
        if statusCode == 200:
            fundType = mfType[:-1]
            break
        else:
            r = None

    if r == None:
        return [statusCode, None]

    data['FundType'] = fundType

    soup = BeautifulSoup(r.text, 'html.parser')

    morningStars = soup.find('span', class_='mdc-security-header__star-rating')
    if morningStars != None:
        data['Rating'] = int(morningStars['title'].split()[0])

    if fundType == 'FUND':
        content = soup.find('div', {'class': 'fund-quote__data'})
        if content == None: return [statusCode, data]
        attrValues = {}
        for item in content.find_all('div', {'class': 'fund-quote__item'}):
            # get attribute names
            label = item.find('div', {'class': 'fund-quote__label'})
            attributes = label.text.strip()
            attributes = attributes.split(' / ')
            
            # get values
            value = item.find('div', {'class': 'fund-quote__value'})
            values = []
            if value != None:
                values = value.text.strip().split('/')
                for i in range(len(values)): values[i] = values[i].strip().replace('\t','').replace('\n',' ')
            else:
                values = item.find_all('span')[-1].text.strip().split('/')
                for i in range(len(values)): values[i] = values[i].strip()
            
            if len(attributes) != len(values): continue
            for i in range(len(attributes)):
                attrValues[attributes[i]] = values[i]
    
        for attr, val in cleanUpAttributes(attrValues).items():
            data[attr] = val
    
    # Only FUND type pages have accessible non JavaScript data

    return [statusCode, data]

def mfQuoteDataMWBS4Proc(quote):
    data = {}
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
    exchange = symbolSplits[1]
    fundStart = 'https://www.marketwatch.com/investing/fund'
    url = '%s/%s?iso=%s' % (fundStart, symbol, exchange)

    r = DS.getRequest(url)
    if r == None:
        statusCode = 500
    else:
        statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, None]

    # only handle data if a fund was found
    urlRedirect = r.url
    if not urlRedirect.upper().startswith(fundStart.upper()):
        return [statusCode, None]

    soup = BeautifulSoup(r.text, 'html.parser')
    
    primary = soup.find('div', {'class': 'region region--primary'})

    foundData = {}
    for ul in primary.find_all('ul'):
        for name in ul.parent.text.split('\n'):
            if name != '': break
        if name == 'Key Data':
            for li in ul.find_all('li'):
                splits = li.text.split('\n')
                foundData[splits[1]] = splits[2]
            continue
        elif name == '': continue
        # print('Unaccounted Data: %s' % name)

    for table in primary.find_all('table'):
        for name in table.parent.text.split('\n'):
            if name != '': break
        if name == 'Fund Details':
            for tr in table.find_all('tr'):
                splits = tr.text.split('\n')
                if splits[2] == '':
                    values = []
                    for value in splits[3:]:
                        if value == '': break
                        values.append(value)
                    foundData[splits[1]] = values
                else:
                    foundData[splits[1]] = splits[2]
            continue
        elif name == 'Fees & Expenses':
            for tr in table.find_all('tr'):
                splits = tr.text.split('\n')
                if splits[2] == '':
                    values = []
                    for value in splits[3:]:
                        if value == '': break
                        values.append(value)
                    foundData[splits[1]] = values
                else:
                    foundData[splits[1]] = splits[2]
            continue
        elif name == 'Risk Measures':
            for tr in table.find_all('tr'):
                splits = tr.text.split('\n')
                if splits[2] == '':
                    values = []
                    for value in splits[3:]:
                        if value == '': break
                        values.append(value)
                    foundData[splits[1]] = values
                else:
                    foundData[splits[1]] = splits[2]
            continue
        elif name == 'Min. Investment':
            for tr in table.find_all('tr'):
                splits = tr.text.split('\n')
                if splits[1] == 'Standard (taxable)':
                    splits[1] = 'Min Investment'
                elif splits[1] == 'IRA':
                    splits[1] = 'Min Investment I R A'
                if splits[2] == '':
                    values = []
                    for value in splits[3:]:
                        if value == '': break
                        values.append(value)
                    foundData[splits[1]] = values
                else:
                    foundData[splits[1]] = splits[2]
            continue
        elif name == 'Distributions':
            for tr in table.find_all('tr'):
                splits = tr.text.split('\n')
                if splits[2] == '':
                    values = []
                    for value in splits[3:]:
                        if value == '': break
                        values.append(value)
                    foundData[splits[1]] = values
                else:
                    foundData[splits[1]] = splits[2]
            continue
        elif name == 'Distribution History':
            income = {}
            capitaGains = {}
            for tr in table.find_all('tr')[1:]:
                splits = tr.text.split('\n')
                income[splits[1]] = splits[2]
                capitaGains[splits[1]] = splits[3]
            income = cleanUpAttributes(income)
            capitaGains = cleanUpAttributes(capitaGains)
            data['DistributionHistory'] = {}
            data['DistributionHistory']['IncomeDividend'] = income
            data['DistributionHistory']['CapitalGains'] = capitaGains
            continue
        elif name == 'Top 10 Holdings':
            holdings = {}
            for tr in table.find_all('tr')[1:]:
                splits = tr.text.split('\n')
                holdings[splits[4]] = splits[5]
            holdings = cleanUpAttributes(holdings, upper=True)
            data['Holdings'] = holdings
            continue
        elif name == '': continue
        # print('Unaccounted Data: %s' % name)
    
    # # <span class="company__market">U.S.: Nasdaq</span>
    # market = soup.find_all('span', {'class': 'company__market'})
    # for element in market:
    #     print(element.text)

    foundData = cleanUpAttributes(foundData)
    for attr, value in foundData.items():
        data[attr] = value

    return [statusCode, data]

def mfHoldingsDataMWBS4Proc(quote):
    data = {}
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
    exchange = symbolSplits[1]
    fundStart = 'https://www.marketwatch.com/investing/fund'
    url = '%s/%s/holdings?iso=%s' % (fundStart, symbol, exchange)

    r = DS.getRequest(url)
    if r == None:
        statusCode = 500
    else:
        statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, None]

    # only handle data if a fund was found
    urlRedirect = r.url
    if not urlRedirect.upper().startswith(fundStart.upper()):
        return [statusCode, None]

    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Asset allocation
    assetAllocation = {}
    table = soup.find('table', {'aria-label': 'Asset allocation data table'})
    if table != None:
        for tr in table.find_all('tr'):
            tds = tr.find_all('td')
            assetAllocation[tds[0].text] = tds[1].text
    assetAllocation = cleanUpAttributes(assetAllocation)
    data['AssetAllocation'] = {}
    for attr, value in assetAllocation.items():
        data['AssetAllocation'][attr] = value

    # Sector allocation
    sectorAllocation = {}
    table = soup.find('table', {'aria-label': 'sector allocation data table'})
    if table != None:
        for tr in table.find_all('tr'):
            tds = tr.find_all('td')
            sectorAllocation[tds[0].text] = tds[1].text
    sectorAllocation = cleanUpAttributes(sectorAllocation)
    data['SectorAllocation'] = {}
    for attr, value in sectorAllocation.items():
        data['SectorAllocation'][attr] = value

    # holdings
    holdings = {}
    div = soup.find('div', {'class': 'element element--table holdings'})
    if div != None:
        tbody = div.find('tbody')
        if tbody != None:
            for tr in tbody.find_all('tr'):
                tds = tr.find_all('td')
                holdings[tds[1].text] = tds[2].text
    holdings = cleanUpAttributes(holdings, upper=True)
    data['Holdings'] = {}
    for attr, value in holdings.items():
        data['Holdings'][attr] = value

    return [200, data]

def mfHoldingsDataMWBS4ProcOLD(quote):
    data = {}
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
    countryCode = symbolSplits[2]
    url = 'https://www.marketwatch.com/investing/fund/%s/holdings?countrycode=%s' % (symbol, countryCode)
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        return [statusCode, {}]

    # check url redirection for useful data or no data at all
    urlRedirect = r.url
    if 'search?' in urlRedirect:
        # symbol was not found
        return [statusCode, {}]
    tail = urlRedirect.replace('https://www.marketwatch.com/','')
    subdata = tail.split('/')
    data['Type'] = subdata[1]

    soup = BeautifulSoup(r.text, 'html.parser')

    # Asset allocation
    assetAllocation = {}
    table = soup.find('table', {'aria-label': 'Asset allocation data table'})
    if table != None:
        for tr in table.find_all('tr'):
            tds = tr.find_all('td')
            assetAllocation[tds[0].text] = tds[1].text
    assetAllocation = cleanUpAttributes(assetAllocation)
    data['AssetAllocation'] = {}
    for attr, value in assetAllocation.items():
        data['AssetAllocation'][attr] = value
    
    # Sector allocation
    sectorAllocation = {}
    table = soup.find('table', {'aria-label': 'sector allocation data table'})
    if table != None:
        for tr in table.find_all('tr'):
            tds = tr.find_all('td')
            sectorAllocation[tds[0].text] = tds[1].text
    sectorAllocation = cleanUpAttributes(sectorAllocation)
    data['SectorAllocation'] = {}
    for attr, value in sectorAllocation.items():
        data['SectorAllocation'][attr] = value

    # holdings
    holdings = {}
    div = soup.find('div', {'class': 'element element--table holdings'})
    if div != None:
        tbody = div.find('tbody')
        if tbody != None:
            for tr in tbody.find_all('tr'):
                tds = tr.find_all('td')
                holdings[tds[1].text] = tds[2].text
    holdings = cleanUpAttributes(holdings, upper=True)
    data['Holdings'] = {}
    for attr, value in holdings.items():
        data['Holdings'][attr] = value
  
    return [statusCode, data]

def mfQuoteDataYFBS4Proc(quote):
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
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
            if attribute == 'Morningstar Rating':
                attributes[attribute] = tds[1].contents[0].text
            else:
                attributes[attribute] = tds[1].text
    
    for attr, value in cleanUpAttributes(attributes).items():
        data[attr] = value
    
    return [statusCode, data]

def mfHoldingsDataYFBS4Proc(quote):
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
    excludeList = ['DIGI']
    if symbol in excludeList:
        logging.info('excluding symbol: %s' % symbol)
        return [200, {}]
    
    data = {}
    url = 'https://finance.yahoo.com/quote/%s/holdings' % symbol
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
        elif not '/holdings' in urlRedirect:
            return [statusCode, {}]

    soup = BeautifulSoup(r.text, 'html.parser')
    section = soup.find_all('section')[1]
    mainBlocks = section.contents
    subBlocks = mainBlocks[0].contents
    for subBlock in subBlocks:
        if subBlock.text.startswith('Overall Portfolio Composition'):
            # Asset allocation
            assetAllocation = {}
            attributes = subBlock.find_all('span', {'class': 'Fl(start)'})
            values = subBlock.find_all('span', {'class': 'Fl(end)'})
            for i in range(len(attributes)):
                assetAllocation[attributes[i].text] = values[i].text
            assetAllocation = cleanUpAttributes(assetAllocation)
            data['AssetAllocation'] = {}
            for attr, value in assetAllocation.items():
                data['AssetAllocation'][attr] = value
        elif subBlock.text.startswith('Sector Weightings'):
            # Sector allocation
            sectorAllocation = {}
            for div in subBlock.contents[1].contents[1:]:
                elements = div.contents
                sectorAllocation[elements[0].text] = elements[2].text
            sectorAllocation = cleanUpAttributes(sectorAllocation)
            data['SectorAllocation'] = {}
            for attr, value in sectorAllocation.items():
                data['SectorAllocation'][attr] = value
    subBlocks = mainBlocks[1].contents
    for subBlock in subBlocks:
        if subBlock.text.startswith('Equity Holdings'):
            pass
        elif subBlock.text.startswith('Bond Holdings'):
            pass
        elif subBlock.text.startswith('Bond Ratings'):
            # bond ratings
            bondRatings = {}
            for div in subBlock.contents[1].contents[1:]:
                elements = div.contents
                bondRatings[elements[0].text] = elements[1].text
            bondRatings = cleanUpAttributes(bondRatings)
            data['BondRatings'] = {}
            for attr, value in bondRatings.items():
                data['BondRatings'][attr] = value

    if len(mainBlocks) > 3:
        holdingsDiv = mainBlocks[3]
        if holdingsDiv.text.startswith('Top 10 Holdings'):
            # holdings
            holdings = {}
            tbody = holdingsDiv.find('tbody')
            for tr in tbody.find_all('tr'):
                tds = tr.find_all('td')
                holdings[tds[1].text] = tds[2].text
            holdings = cleanUpAttributes(holdings, upper=True)
            data['Holdings'] = {}
            for attr, value in holdings.items():
                data['Holdings'][attr] = value
    
    return [statusCode, data]

def mfQuoteInfoYFProc(quote):
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
    ticker = yf.Ticker(symbol)
    return ticker.info

# def mfHoldingsDataMSSELProc(quoteVar, driver):
#     return None
#     # data = {}
#     # qvSplit = quoteVar[0].split(':')s
#     # symbol = qvSplit[0]
#     # exchange = qvSplit[1]
#     # type = quoteVar[1]

#     # # XNYS: https://www.morningstar.com/cefs/xnys/PAXS/quote
#     # # XNYS: https://www.morningstar.com/stocks/xnys/MVO/quote
#     # # XASE: https://www.morningstar.com/cefs/xase/AEF/quote
#     # # ARCX: https://www.morningstar.com/etfs/arcx/SPSM/quote
#     # # BATS: https://www.morningstar.com/etfs/bats/STOTS/quote
#     # # XNAS: https://www.morningstar.com/funds/xnas/VITAX/quote
#     # # XNAS: https://www.morningstar.com/etfs/xnas/SOGU/quote
#     # # XNAS: https://www.morningstar.com/stocks/xnas/HRZN/quote
#     # # XETR: None LMWE
#     # # OOTC: https://www.morningstar.com/stocks/pinx/AWRRF/quote
#     # # OOTC: None ISMXF

#     # # https://www.morningstar.com/ETFS/XNAS/EMXF/quote

#     # url = 'https://www.morningstar.com/%s/%s/%s/quote' % (type+'S', exchange, symbol)
    
#     # r = DS.getRequest(url)
#     # statusCode = r.status_code

#     # if statusCode != 200:
#     #     return [statusCode, data]
    
#     # soup = BeautifulSoup(r.text, 'html.parser')

#     # morningStars = soup.find('span', class_='mdc-security-header__star-rating')
#     # if morningStars != None:
#     #     data['MorningStars'] = int(morningStars['title'].split()[0])

#     # if type == 'FUND':
#     #     content = soup.find('div', {'class': 'fund-quote__data'})
#     #     if content == None: return [statusCode, data]
#     #     for item in content.find_all('div', {'class': 'fund-quote__item'}):
#     #         # get attribute names
#     #         label = item.find('div', {'class': 'fund-quote__label'})
#     #         attributes = label.text.split('\n')
#     #         if len(attributes) > 1:
#     #             attributes = attributes[1]
#     #         else:
#     #             attributes = attributes[0]
#     #         attributes = attributes.replace('\t','')
#     #         attributes = attributes.split(' / ')

#     #         # get values
#     #         value = item.find('div', {'class': 'fund-quote__value'})
#     #         values = []
#     #         if value != None:
#     #             for span in value.find_all('span'):
#     #                 spanValues = []
#     #                 for spanValue in span.text.split('\n'):
#     #                     spanValues.append(spanValue.replace('\t',''))
#     #                 values.append(''.join(spanValues))
#     #         else:
#     #             values.append(item.find_all('span')[-1].text)

#     #         # add to data
#     #         aIndex = 0
#     #         cleanupAttributes = {}
#     #         for attribute in attributes:
#     #             if attribute == 'Investment Style':
#     #                 attrSplit = attribute.split()
#     #                 valueSplit = values[aIndex].split()
#     #                 data['Style'] = {}
#     #                 data['Style'][attrSplit[0]] = valueSplit[0]
#     #                 if len(valueSplit) >= 2:
#     #                     data['Style'][attrSplit[1]] = valueSplit[1]
#     #                 else:
#     #                     data['Style'][attrSplit[1]] = valueSplit[-1]
#     #             else:
#     #                 if aIndex <= (len(values)-1):
#     #                     cleanupAttributes[attribute] = values[aIndex]
#     #                 else:
#     #                     cleanupAttributes[attribute] = values[-1]
#     #             aIndex += 1
#     #         for attr, value in cleanUpAttributes(cleanupAttributes).items():
#     #             data[attr] = value

#     # elif type == 'ETF':
#     #     content = soup.find('div', {'class': 'etf__content'})
#     #     # data unaccessible with BS4
#     # elif type == 'CEF':
#     #     content = soup.find('div', {'class': 'cef__content'})
#     #     # data unaccessible with BS4
#     # elif type == 'STOCK':
#     #     content = soup.find('div', {'class': 'stock__content'})
#     #     # data unaccessible with BS4
#     # else:
#     #     logging.info('%s: No recognizable type for MorningStar: %s' % (symbol, type))

#     # return [statusCode, data]

# --- MAIN SCRAPERS ---

def getMFBASEData(dataFileName):
    logging.info('Retrieving BASE Data')
    MFData = DS.getData(dataFileName)
    BData = DS.getData('BASE_DATA_SCRAPED')
    
    for dataName in BData.keys():
        MFData[dataName] = BData[dataName]
    
    if not DS.saveData(MFData, dataFileName):
        logging.info('%s: Stop saving data and exit program' % dataFileName)
        exit(0)

def getMFQuotesMWBS4(dataFileName):
    logging.info('Retrieving all Fund Quotes from MarketWatch')
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchQuotes'
    if not dataName in MFData: MFData[dataName] = {}

    # scrape for alphabetic list pages in MW
    letters = [chr(x) for x in range(65, 91)]
    results = DS.multiScrape(letters, marketWatchPagesProc)

    # get MW page url links
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
    if 403 in results[0]:
        logging.info('At least one page was blocked')
        return
    
    # create quotes set
    micSubstitudes = {'': 'XXXX', 'XPLU': 'ISDX'}
    for result in results[1]:
        for data in result:
            splits = data.split(':')
            symbol = splits[0]
            mic = splits[1]
            country = splits[2]
            name = splits[3]
            sector = splits[4]
            if mic in micSubstitudes:
                mic = micSubstitudes[mic]
            quote = '%s:%s' % (symbol, mic)
            if not quote in MFData[dataName]:
                MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            MFData[dataName][quote]['Symbol'] = symbol
            MFData[dataName][quote]['MIC'] = mic
            MFData[dataName][quote]['Country'] = country
            MFData[dataName][quote]['Name'] = name
            MFData[dataName][quote]['Sector'] = sector
    
    logging.info('Total quotes in data: %s' % len(MFData[dataName]))
    
    if not DS.saveData(MFData, dataFileName):
        logging.info('%s: Stop saving data and exit program' % dataFileName)
        exit(0)

def getMFAddSimilarQuotesMSBS4(dataFileName):
    logging.info('Adding similar funds symbol quotes from MornongStar Search')
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchQuotes'
    micSubstitudes = {'': 'XXXX', 'XPLU': 'ISDX'}
    if not dataName in MFData:
        logging.info('No s quotes found in data !' % dataName)
        return
    
    quotes = list(MFData[dataName].keys())
    mwQuoteCount = len(quotes)

    sTotal = len(quotes)
    for block in DS.makeMultiBlocks(quotes, 300):
        logging.info('Symbols search with MorningStar: %s' % sTotal)
        results = DS.multiScrape(block, mfQuoteSearchOtherMSBS4Proc)

        sIndex = 0
        for data in results[1]:
            quote = block[sIndex]
            name = MFData[dataName][quote]['Name']
            for fquote, fname in data.items():
                if not fquote in MFData[dataName]:
                    if similarity(fname, name) > 0.4:
                        splits = fquote.split(':')
                        fsymbol = splits[0]
                        fmic = splits[1]
                        MFData[dataName][fquote] = {}
                        MFData[dataName][fquote]['ScrapeTag'] = datetime.now()
                        MFData[dataName][fquote]['Symbol'] = fsymbol
                        MFData[dataName][fquote]['MIC'] = fmic
                        MFData[dataName][fquote]['Name'] = fname
            sIndex += 1
  
        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)
    
    addCount = len(MFData[dataName].keys()) - mwQuoteCount
    logging.info('Added %s more quotes to %s' % (addCount, dataName))

def getMFQuoteDataMSBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    logging.info('Retrieving quote data of only funds in MorningStar')
    MFData = DS.getData(dataFileName)
    dataName = 'MorningStarQuoteData'
    if not 'MarketWatchQuotes' in MFData:
        logging.info('No MarketWatchQuotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    # find quotes that need to be checked  by MorningStar
    mwQuotes = set(MFData['MarketWatchQuotes'].keys())
    msQuotes = set(MFData[dataName].keys())
    msTodoQuotes = set(quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days))
    quotes = list(mwQuotes.difference(msQuotes.difference(msTodoQuotes)))

    sTotal = len(quotes)
    for block in DS.makeMultiBlocks(quotes, 300):
        logging.info('Scrape quote data with MorningStar: %s' % sTotal)
        results = DS.multiScrape(block, mfQuoteDataMSBS4Proc)

        sIndex = 0
        for data in results[1]:
            quote = block[sIndex]
            if data != None:
                if not quote in MFData[dataName]: MFData[dataName][quote] = {}
                MFData[dataName][quote]['ScrapeTag'] = datetime.now()
                for attr, value in data.items():
                    MFData[dataName][quote][attr] = value
            elif quote in MFData[dataName]:
                logging.info('Taking out quote from MorningStarQuoteData: %s' % quote)
                MFData[dataName].pop(quote)
            sIndex += 1

        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

def getMFQuoteDataMWBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    logging.info('Retrieving quote data of only funds in MarketWatch')
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchQuoteData'
    if not 'MorningStarQuoteData' in MFData:
        logging.info('No MorningStarQuoteData found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    # update quotes that are in MorningStarQuoteData and that need to be updated
    msQuotes = set(MFData['MorningStarQuoteData'].keys())
    mwQuotesNotNeedScrape = set(quotesNeedScrape(MFData, dataName, needScrape=False, seconds=seconds, minutes=minutes, hours=hours, days=days))
    quotes = list(msQuotes.difference(mwQuotesNotNeedScrape))

    sTotal = len(quotes)
    for block in DS.makeMultiBlocks(quotes, 100):
        logging.info('Quotes to scrape quote data with MarketWatch: %s' % sTotal)
        results = DS.multiScrape(block, mfQuoteDataMWBS4Proc, retryStatusCode=403)

        sIndex = 0
        for data in results[1]:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            if data != None:
                for attr, value in data.items():
                    attribute = attributeCheck(attr)
                    MFData[dataName][quote][attribute] = value
            sIndex += 1

        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

def getMFHoldingsDataMWBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    logging.info('Retrieving quote Holdings data of only funds in MarketWatch')
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchHoldingsData'
    if not 'MorningStarQuoteData' in MFData:
        logging.info('No MorningStarQuoteData found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    # update quotes that are in MorningStarQuoteData and that need to be updated
    msQuotes = set(MFData['MorningStarQuoteData'].keys())
    mwQuotesNotNeedScrape = set(quotesNeedScrape(MFData, dataName, needScrape=False, seconds=seconds, minutes=minutes, hours=hours, days=days))
    quotes = list(msQuotes.difference(mwQuotesNotNeedScrape))

    sTotal = len(quotes)
    for block in DS.makeMultiBlocks(quotes, 100):
        logging.info('Quotes to scrape quote Holdings data with MarketWatch: %s' % sTotal)
        results = DS.multiScrape(block, mfHoldingsDataMWBS4Proc, retryStatusCode=403)

        sIndex = 0
        for data in results[1]:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            if data != None:
                for attr, value in data.items():
                    attribute = attributeCheck(attr)
                    MFData[dataName][quote][attribute] = value
            sIndex += 1

        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

def getMFHoldingsDataMWBS4OLD(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchHoldingsData'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    sTotal = len(todoQuotes)
    for block in DS.makeMultiBlocks(todoQuotes, 100):
        logging.info('quotes to scrape holdings data with MarketWatch: %s' % sTotal)
        results = DS.multiScrape(block, mfHoldingsDataMWBS4Proc, retryStatusCode=403)

        sIndex = 0
        for data in results[1]:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                MFData[dataName][quote][attribute] = value
            sIndex += 1

        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

def getMFQuoteDataYFBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'YahooFinanceQuoteData'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    sTotal = len(todoQuotes)
    for block in DS.makeMultiBlocks(todoQuotes, 10):
        logging.info('quotes to scrape quote data with Yahoo Finance: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfQuoteDataYFBS4Proc, retryStatusCode=404)

        sIndex = 0
        for data in quoteData[1]:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                MFData[dataName][quote][attribute] = value
            sIndex += 1

        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

def getMFHoldingsDataYFBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'YahooFinanceHoldingsData'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    sTotal = len(todoQuotes)
    for block in DS.makeMultiBlocks(todoQuotes, 10):
        logging.info('quotes to scrape holdings data with with Yahoo Finance: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfHoldingsDataYFBS4Proc, retryStatusCode=404)

        sIndex = 0
        for data in quoteData[1]:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                MFData[dataName][quote][attribute] = value
            sIndex += 1
        
        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

def getMFQuoteInfoYF(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'YFinanceTickerInfo'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    processes = 6
    sleepStepTime = 30
    multiPool = Pool(processes)
    sTotal = len(todoQuotes)
    for block in DS.makeMultiBlocks(todoQuotes, processes):
        logging.info('quotes to get quote info with YFinance: %s' % sTotal)
        results = []
        retry = True
        while retry:
            results = multiPool.map(mfQuoteInfoYFProc, block)
            retry = False

            # test if locked
            totalSleep = 0
            while len(mfQuoteInfoYFProc('VITAX:XNAS:US')) < 4:
                retry = True
                time.sleep(sleepStepTime)
                totalSleep += sleepStepTime
                logging.info('waiting for %s seconds to unblock' % totalSleep)
            if retry:
                logging.info('retry same quotes to scrape quote data with Yahoo Finance')

        sIndex = 0
        for data in results:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            MFData[dataName][quote]['Info'] = data
            sIndex += 1

        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

# def getMFHoldingsDataMSSEL(dataFileName, seconds=0, minutes=0, hours=0, days=0):
#     MFData = DS.getData(dataFileName)
#     dataName = 'MorningStarHoldingsData'
#     if not 'Quotes' in MFData:
#         logging.info('No quotes found in data !')
#         return
#     if not 'MorningStarTypes' in MFData:
#         logging.info('Could not retrieve Type data !')
#         return
#     if not dataName in MFData: MFData[dataName] = {}

#     todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)
#     todoQuotes = set(todoQuotes)
#     quoteTypes = set()
#     for quote, data in MFData['MorningStarTypes'].items():
#         if data['Type'] != None: quoteTypes.add(quote)
#     todoQuotes = list(quoteTypes.intersection(set(todoQuotes)))
#     todoQuotes.sort()
#     todoQuoteType = []
#     for quote in todoQuotes:
#         todoQuoteType.append([quote, MFData['MorningStarTypes'][quote]['Type']])
    
#     todoQuoteType = [['VITAX:XNAS:US', 'FUND']]

#     # driverCount = 8
#     driverCount = 4
#     scrapesPerDriver = 5

#     drivers = DS.startWebdrivers(driverCount)

#     sTotal = len(todoQuoteType)
#     for block in DS.makeMultiBlocks(todoQuoteType, (driverCount * scrapesPerDriver)):
#         logging.info('quotes to scrape holdings data with with MorningStar: %s' % sTotal)
#         logging.info(block)
#         quoteData = DS.multiWebdriversScrape(block, mfHoldingsDataMSSELProc, drivers)
#         logging.info(quoteData)

#         # sIndex = 0
#         # for data in quoteData[1]:
#         #     quote = block[sIndex][0]
#         #     if not quote in MFData[dataName]: MFData[dataName][quote] = {}
#         #     MFData[dataName][quote]['ScrapeTag'] = datetime.now()
#         #     for attr, value in data.items():
#         #         attribute = attributeCheck(attr)
#         #         MFData[dataName][quote][attribute] = value
#         #     sIndex += 1

        # if not DS.saveData(MFData, dataFileName):
        #     logging.info('%s: Stop saving data and exit program' % dataFileName)
        #     exit(0)
        
#         sTotal = sTotal - len(block)

#     DS.quitWebDrivers(drivers)

def getMFQuoteDataETAPI(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'ETradeQuoteData'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    if len(todoQuotes) == 0: return
    session = ET.getSession()

    sTotal = len(todoQuotes)
    for block in DS.makeMultiBlocks(todoQuotes, 10):
        symbols = [quote.split(':')[0] for quote in block]
        logging.info('quotes to scrape data with with ETrade API: %s' % sTotal)
        quoteData = ET.multiQuotes(symbols, session, detailFlag='MF_DETAIL')

        sIndex = 0
        for data in quoteData:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            for attr, value in data.items():
                attribute = attributeCheck(attr)
                MFData[dataName][quote][attribute] = value
            sIndex += 1
        
        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            ET.endSession(session)
            exit(0)
        
        sTotal = sTotal - len(block)
    
    ET.endSession(session)

if __name__ == "__main__":
    scrapedFileName = 'MF_DATA_SCRAPED'
    historyUpdateDays = 10
    DS.setupLogging('MFDataScrape.log', timed=True, new=True)

    # # copy Base Data to Mutual Fund Data
    # getMFBASEData(scrapedFileName)

    # # create MFData by retrieving mutual funds from MarketWatch
    # # 40504 quotes = 00:00:13
    # getMFQuotesMWBS4(scrapedFileName)

    # # Search for more exchanges on symbols through MorningStar
    # # 40504 quotes = 00:25:53
    # getMFAddSimilarQuotesMSBS4(scrapedFileName)

    # # get quote data from MorningStar
    # # 40794 quotes = 00:40:32
    # getMFQuoteDataMSBS4(scrapedFileName, days=historyUpdateDays)

    # # get quote data from MarketWatch
    # # 37037 quotes = 07:09:45
    # getMFQuoteDataMWBS4(scrapedFileName, days=historyUpdateDays)
    
    # get holdings data from MarketWatch
    # slow because of blocking
    getMFHoldingsDataMWBS4(scrapedFileName, days=historyUpdateDays)

    
    # MFData = DS.getData(scrapedFileName)

    # print(len(MFData['MarketWatchQuotes']))
    # print(len(MFData['MorningStarQuoteData']))
    # print(len(MFData['MarketWatchQuoteData']))

    # for quote, data in MFData['MarketWatchQuoteData'].items():
    #     if len(data) < 2:
    #         logging.info('%s: %s' % (quote, data))

    # MFData.pop('MarketWatchQuoteData')
    # DS.saveData(MFData, scrapedFileName)



    # # get quote data from YahooFinance
    # # slow because of blocking
    # getMFQuoteDataYFBS4(scrapedFileName, days=historyUpdateDays)

    # # get holdings data from YahooFinance
    # # slow because of blocking
    # getMFHoldingsDataYFBS4(scrapedFileName, days=historyUpdateDays)

    # # get quote info from YahooFinance
    # # very slow because of blocking, slowest of them all
    # getMFQuoteInfoYF(scrapedFileName, days=historyUpdateDays)

    # # in progress
    # # get holdings data from MorningStar, needs Selenium drivers
    # getMFHoldingsDataMSSEL(scrapedFileName, days=historyUpdateDays)

    # # get holdings data from YahooFinance
    # # slow because of blocking
    # getMFQuoteDataETAPI(scrapedFileName, days=historyUpdateDays)
