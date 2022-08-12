import DataScrape as DS
import ETradeAPI as ET
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

def quotesNeedScrape(MFData, dataName, seconds=0, minutes=0, hours=0, days=0):
    if not 'Quotes' in MFData: return []
    quotes = list(MFData['Quotes'])
    quotes.sort()
    if not dataName in MFData: return quotes
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
    quotes = list(MFData['Quotes'].difference(quotesDone))
    quotes.sort()
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
        exchange = tds[2].text
        sector = None
        if len(tds) > 3:
            sector = tds[3].text
        data.append('%s:%s:%s:%s:%s' % (symbol, exchange, country, name, sector))

    return [statusCode, data]

def mfTypeMSBS4Proc(quote):
    msTypes = ['FUNDS', 'CEFS', 'ETFS', 'STOCKS']
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
    exchange = symbolSplits[1]
    scode = None
    for msType in msTypes:
        url = 'https://www.morningstar.com/%s/%s/%s/quote' % (msType, exchange, symbol)
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

def mfQuoteDataMSBS4Proc(quoteVar):
    data = {}
    qvSplit = quoteVar[0].split(':')
    symbol = qvSplit[0]
    exchange = qvSplit[1]
    type = quoteVar[1]

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

    # https://www.morningstar.com/ETFS/XNAS/EMXF/quote

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

def mfQuoteDataMWBS4Proc(quote):
    data = {}
    symbolSplits = quote.split(':')
    symbol = symbolSplits[0]
    countryCode = symbolSplits[2]
    url = 'https://www.marketwatch.com/investing/fund/%s?countrycode=%s' % (symbol, countryCode)
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
        # logging.info('Unaccounted Data: %s' % name)

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
        # logging.info('Unaccounted Data: %s' % name)
    
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

def getMFExchangeISOBS4(dataFileName):
    MFData = DS.getData(dataFileName)
    if not 'ExchangeCodes' in MFData: MFData['ExchangeCodes'] = {}

    url = 'https://www.iotafinance.com/en/ISO-10383-Market-Identification-Codes-MIC.html'
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        logging.info('Could not find Stock Exchange ISO codes !')
        return
    
    soup = BeautifulSoup(r.text, 'html.parser')

    for element in soup.find_all('li', {'class': 'mic-list-element'}):
        names = element.find('div', {'class': 'row'}).find_all('div')
        MFData['ExchangeCodes'][names[0].text.strip()] = names[2].text.strip()

    # # some are missing
    # MFData['CountryCodes']['South Korea'] = 'KR'
    # MFData['CountryCodes']['Vietnam'] = 'VN'

    if not DS.saveData(MFData, dataFileName):
        logging.info('%s: Stop saving data and exit program' % dataFileName)
        exit(0)

def getMFCountryISOBS4(dataFileName):
    MFData = DS.getData(dataFileName)
    if not 'CountryCodes' in MFData: MFData['CountryCodes'] = {}

    url = 'https://www.iban.com/country-codes'
    r = DS.getRequest(url)
    statusCode = r.status_code

    if statusCode != 200:
        logging.info('Could not find Country ISO codes !')
        return
    
    soup = BeautifulSoup(r.text, 'html.parser')

    table = soup.find('table', {'id': 'myTable'})

    for tr in table.find_all('tr'):
        elements = tr.text.split('\n')
        if elements[1] == 'Country': continue
        MFData['CountryCodes'][elements[1]] = elements[2]
    
    # some are missing
    MFData['CountryCodes']['South Korea'] = 'KR'
    MFData['CountryCodes']['Vietnam'] = 'VN'

    if not DS.saveData(MFData, dataFileName):
        logging.info('%s: Stop saving data and exit program' % dataFileName)
        exit(0)

def getMFQuotesMWBS4(dataFileName):
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchQuotes'
    if not 'CountryCodes' in MFData:
        logging.info('Did not find country codes in data. Aborting !')
        return
    if not 'Quotes' in MFData: MFData['Quotes'] = set()
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
    if 403 in results[0]:
        logging.info('PAge was blocked')

    for result in results[1]:
        for data in result:
            splits = data.split(':')
            symbol = splits[0]
            exchange = splits[1]
            country = splits[2]
            name = splits[3]
            sector = splits[4]
            countryCode = None
            for countryName, code in MFData['CountryCodes'].items():
                if country in countryName:
                    countryCode = code
            quote = '%s:%s:%s' % (symbol, exchange, countryCode)
            MFData['Quotes'].add(quote)
            if not quote in MFData[dataName]:
                MFData[dataName][quote] = {}
            MFData[dataName][quote]['Name'] = name
            MFData[dataName][quote]['Country'] = country
            MFData[dataName][quote]['Sector'] = sector
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
    
    logging.info('Total quotes in data: %s' % len(MFData['Quotes']))
    
    if not DS.saveData(MFData, dataFileName):
        logging.info('%s: Stop saving data and exit program' % dataFileName)
        exit(0)

def getMFTypeMSBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'MorningStarTypes'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    sTotal = len(todoQuotes)
    for block in DS.makeMultiBlocks(todoQuotes, 300):
        logging.info('symbols to scrape types with MorningStar: %s' % sTotal)
        exchangeData = DS.multiScrape(block, mfTypeMSBS4Proc)

        sIndex = 0
        for data in exchangeData[1]:
            quote = block[sIndex]
            if not quote in MFData[dataName]: MFData[dataName][quote] = {}
            MFData[dataName][quote]['ScrapeTag'] = datetime.now()
            attribute = attributeCheck('Type')
            if data != None:
                data = data[:-1]
            MFData[dataName][quote]['Type'] = data
            sIndex += 1
        
        if not DS.saveData(MFData, dataFileName):
            logging.info('%s: Stop saving data and exit program' % dataFileName)
            exit(0)
        
        sTotal = sTotal - len(block)

def getMFQuoteDataMSBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'MorningStarQuoteData'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not 'MorningStarTypes' in MFData:
        logging.info('Could not retrieve Type data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)
    todoQuotes = set(todoQuotes)
    quoteTypes = set()
    for quote, data in MFData['MorningStarTypes'].items():
        if data['Type'] != None: quoteTypes.add(quote)
    todoQuotes = list(quoteTypes.intersection(set(todoQuotes)))
    todoQuotes.sort()
    todoQuoteType = []
    for quote in todoQuotes:
        todoQuoteType.append([quote, MFData['MorningStarTypes'][quote]['Type']])

    sTotal = len(todoQuoteType)
    for block in DS.makeMultiBlocks(todoQuoteType, 300):
        logging.info('quotes to scrape quote data with MorningStar: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfQuoteDataMSBS4Proc)

        sIndex = 0
        for data in quoteData[1]:
            quote = block[sIndex][0]
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

def getMFQuoteDataMWBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
    MFData = DS.getData(dataFileName)
    dataName = 'MarketWatchQuoteData'
    if not 'Quotes' in MFData:
        logging.info('No quotes found in data !')
        return
    if not dataName in MFData: MFData[dataName] = {}

    todoQuotes = quotesNeedScrape(MFData, dataName, seconds=seconds, minutes=minutes, hours=hours, days=days)

    sTotal = len(todoQuotes)
    for block in DS.makeMultiBlocks(todoQuotes, 100):
        logging.info('quotes to scrape quote data with MarketWatch: %s' % sTotal)
        quoteData = DS.multiScrape(block, mfQuoteDataMWBS4Proc, retryStatusCode=403)

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

def getMFHoldingsDataMWBS4(dataFileName, seconds=0, minutes=0, hours=0, days=0):
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

    # # retrieving country ISO codes
    # getMFCountryISOBS4(scrapedFileName)

    # # retrieving stock exchange ISO codes
    # getMFExchangeISOBS4(scrapedFileName)

    # # create MFData by retrieving mutual funds from MarketWatch
    # getMFQuotesMWBS4(scrapedFileName)

    # # get investment type fom MorningStar links
    # # fast (1h 23 min for 38043 quotes) (29 min for 38040 quotes)
    # getMFTypeMSBS4(scrapedFileName, days=historyUpdateDays)

    # # get quote data from MorningStar
    # # fast
    # getMFQuoteDataMSBS4(scrapedFileName, days=historyUpdateDays)

    # # get quote data from MarketWatch
    # # slow because of blocking
    # getMFQuoteDataMWBS4(scrapedFileName, days=historyUpdateDays)

    # # get holdings data from MarketWatch
    # # slow because of blocking
    # getMFHoldingsDataMWBS4(scrapedFileName, days=historyUpdateDays)

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
