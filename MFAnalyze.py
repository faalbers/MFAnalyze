import DataScrape as DS
import logging
import ExchangeInfo as EI

# main program
if __name__ == "__main__":
    DS.setupLogging('MFAnalyze.log', timed=False, new=True)

    dataFileName = 'MF_DATA'
    scrapedFileName = 'MF_DATA_SCRAPED'
    MFSData = DS.getData(scrapedFileName)

    # attributes check

    subNameAttributes = {}
    for subName, subData in MFSData.items():
        if subName == 'Symbols': continue
        subNameAttributes[subName] = set()
        for symbol, data in subData.items():
            for attr in data.keys(): subNameAttributes[subName].add(attr)
    for subName, attributes in subNameAttributes.items():
        attrList = list(attributes)
        attrList.sort()
        logging.info('%s:' % subName)
        for attr in attrList:
            logging.info('    - %s' % attr)

    # Types check

    types = set()
    etfs = set()
    cefs = set()
    funds = set()
    stocks = set()
    nones = set()
    for symbol, data in MFSData['MorningStarTypes'].items():
        if 'Type' in data:
            types.add(data['Type'])
            if data['Type'] == None: nones.add(symbol)
            if data['Type'] == 'ETF': etfs.add(symbol)
            if data['Type'] == 'CEF': cefs.add(symbol)
            if data['Type'] == 'FUND': funds.add(symbol)
            if data['Type'] == 'STOCK': stocks.add(symbol)
    logging.info('MorningStarTypes:')
    logging.info('    %s' % types)

    types = set()
    for symbol, data in MFSData['MarketWatchQuoteData'].items():
        if 'Type' in data:
            types.add(data['Type'])
    logging.info('MarketWatchQuoteData types:')
    logging.info('    %s' % types)

    print(len(etfs))
    print(len(cefs))
    print(len(funds))
    print(len(stocks))

    etfs = set(MFSData['MarketWatchQuoteData'].keys()).intersection(etfs)
    etfTypes = set()
    for etf in etfs:
        if 'Type' in MFSData['MarketWatchQuoteData'][etf]:
            etfTypes.add(MFSData['MarketWatchQuoteData'][etf]['Type'])
    logging.info('Types in ETF symbols:')
    logging.info('    %s' % etfTypes)
    
    cefs = set(MFSData['MarketWatchQuoteData'].keys()).intersection(cefs)
    cefTypes = set()
    for cef in cefs:
        if 'Type' in MFSData['MarketWatchQuoteData'][cef]:
            cefTypes.add(MFSData['MarketWatchQuoteData'][cef]['Type'])
    cefTypes = list(cefTypes)
    cefTypes.sort()
    logging.info('Types in CEF symbols:')
    logging.info('    %s' % cefTypes)
    
    funds = set(MFSData['MarketWatchQuoteData'].keys()).intersection(funds)
    fundTypes = set()
    for fund in funds:
        if 'Type' in MFSData['MarketWatchQuoteData'][fund]:
            fundTypes.add(MFSData['MarketWatchQuoteData'][fund]['Type'])
    fundTypes = list(fundTypes)
    fundTypes.sort()
    logging.info('Types in FUND symbols:')
    logging.info('    %s' % fundTypes)

    stocks = set(MFSData['MarketWatchQuoteData'].keys()).intersection(stocks)
    stockTypes = set()
    for stock in stocks:
        if 'Type' in MFSData['MarketWatchQuoteData'][stock]:
            stockTypes.add(MFSData['MarketWatchQuoteData'][stock]['Type'])
    stockTypes = list(stockTypes)
    stockTypes.sort()
    logging.info('Types in STOCK symbols:')
    logging.info('    %s' % stockTypes)

    # categories check

    categories = set()
    for symbol, data in MFSData['MorningStarQuoteData'].items():
        if 'Category' in data and data['Category'] != None:
            categories.add(data['Category'])
    MSQDCat = categories.copy()
    categories = list(categories)
    categories.sort()
    logging.info('')
    logging.info('MorningStarQuoteData Categories')
    for category in categories:
        logging.info('    - %s' % category)

    categories = set()
    for symbol, data in MFSData['MarketWatchQuoteData'].items():
        if 'Category' in data and data['Category'] != None:
            categories.add(data['Category'])
    categories = list(categories)
    categories.sort()
    logging.info('')
    logging.info('MarketWatchQuoteData Categories')
    for category in categories:
        logging.info('    - %s' % category)

    categories = set()
    for symbol, data in MFSData['YahooFinanceQuoteData'].items():
        if 'Category' in data and data['Category'] != None:
            categories.add(data['Category'])
    YFQDCat = categories.copy()
    categories = list(categories)
    categories.sort()
    logging.info('')
    logging.info('YahooFinanceQuoteData Categories')
    for category in categories:
        logging.info('    - %s' % category)

    categories = YFQDCat.intersection(MSQDCat)
    categories = list(categories)
    categories.sort()
    logging.info('')
    logging.info('MorningStarQuoteData and YahooFinanceQuoteData same named Categories')
    for category in categories:
        logging.info('    - %s' % category)

    categories = set()
    for symbol, data in MFSData['YFinanceTickerInfo'].items():
        if 'category' in data['Info'] and data['Info']['category'] != None:
            categories.add(data['Info']['category'])
    YFTICat = categories.copy()
    categories = list(categories)
    categories.sort()
    logging.info('')
    logging.info('YFinanceTickerInfo Categories')
    for category in categories:
        logging.info('    - %s' % category)
    
    categories = YFQDCat.intersection(YFTICat)
    categories = list(categories)
    categories.sort()
    logging.info('')
    logging.info('YahooFinanceQuoteData and YFinanceTickerInfo same named Categories')
    for category in categories:
        logging.info('    - %s' % category)
   
    # sectors check

    sectors = set()
    for symbol, data in MFSData['MarketWatchQuotes'].items():
        sectors.add(data['Sector'])
    YFQDCat = sectors.copy()
    sectors = list(sectors)
    sectors.sort()
    logging.info('')
    logging.info('MarketWatchQuotes Sectors')
    for sector in sectors:
        logging.info('    - %s' % sector)

    # YFinance info attributes

    attributes = set()
    for symbol, data in MFSData['YFinanceTickerInfo'].items():
        for attribute in data['Info'].keys():
            attributes.add(attribute)
    attributes = list(attributes)
    attributes.sort()
    logging.info('')
    logging.info('YFinanceTickerInfo Info attributes')
    for attr in attributes:
        logging.info('    - %s' % attr)

    # get style data
    
    iTypes = set()
    sTypes = set()
    for symbol, data in MFSData['MorningStarQuoteData'].items():
        if 'Style' in data:
            for type, value in data['Style'].items():
                if type == 'Investment':  iTypes.add(value)
                if type == 'Style':  sTypes.add(value)
    logging.info('')
    logging.info('MorningStarQuoteData Investment Types')
    for type  in iTypes:
        logging.info('    - %s' % type)
    logging.info('MorningStarQuoteData Style Types')
    for type  in sTypes:
        logging.info('    - %s' % type)

    # types with styles
    msTypes = set()
    mwTypes = set()
    for symbol, data in MFSData['MorningStarQuoteData'].items():
        if 'Style' in data:
            if symbol in MFSData['MorningStarTypes']:
                msTypes.add(MFSData['MorningStarTypes'][symbol]['Type'])
            if symbol in MFSData['MarketWatchQuoteData']:
                if 'Type' in MFSData['MarketWatchQuoteData'][symbol]:
                    mwTypes.add(MFSData['MarketWatchQuoteData'][symbol]['Type'])
    logging.info('')
    logging.info('MMorningStarTypes Types that have Style')
    for type  in msTypes:
        logging.info('    - %s' % type)
    logging.info('')
    logging.info('MarketWatchQuoteData Types that have Style')
    for type  in mwTypes:
        logging.info('    - %s' % type)

    # check allocation types
    msTypes = set()
    mwTypes = set()
    for symbol, data in MFSData['YahooFinanceQuoteData'].items():
        if 'Category' in data and data['Category'] != None:
            if data['Category'].startswith('Allocation'):
                msTypes.add(MFSData['MorningStarTypes'][symbol]['Type'])
                if "Type" in MFSData['MarketWatchQuoteData'][symbol]:
                    mwTypes.add(MFSData['MarketWatchQuoteData'][symbol]['Type'])
    logging.info('')
    logging.info('YahooFinanceQuoteData/Category that starts with "Allocation" are of types')
    logging.info('    MorningStarTypes/Type    : %s' % msTypes)
    logging.info('    MarketWatchQuoteData/Type: %s' % mwTypes)

    # all exchanges
    exchanges = set()
    usExchanges = set()
    for symbol, data in MFSData['MarketWatchQuotes'].items():
        exchanges.add(data['Exchange'])
        if data['Country'] == 'United States':
            usExchanges.add(data['Exchange'])
        
    exchanges = list(exchanges)
    exchanges.sort()
    usExchanges = list(usExchanges)
    usExchanges.sort()
    logging.info('')
    logging.info('MarketWatchQuotes/Exchange')
    for exchange  in exchanges:
        if exchange in EI.Exchanges:
            logging.info('    - %s: %s' % (exchange, EI.Exchanges[exchange]))
        else:
            logging.info('    - %s: Undefined' % exchange)
    logging.info('')
    logging.info('MarketWatchQuotes/Exchange USA')
    for exchange  in usExchanges:
        if exchange in EI.Exchanges:
            logging.info('    - %s: %s' % (exchange, EI.Exchanges[exchange]))
        else:
            logging.info('    - %s: Undefined' % exchange)

