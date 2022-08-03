import DataScrape as DS
import logging
import ExchangeInfo as EI

# main program
if __name__ == "__main__":
    DS.setupLogging('MFAnalyze.log', timed=False, new=True)

    scrapedFileName = 'MF_DATA_SCRAPED'
    MFSData = DS.getData(scrapedFileName)

    # attributes check

    subNameAttributes = {}
    for subName, subData in MFSData.items():
        subNameAttributes[subName] = set()
        if type(subData) == dict:
            for quote, data in subData.items():
                if type(data) == dict:
                    for attr in data.keys(): subNameAttributes[subName].add(attr)
    for subName, attributes in subNameAttributes.items():
        attrList = list(attributes)
        attrList.sort()
        logging.info('')
        logging.info('%s:' % subName)
        for attr in attrList:
            logging.info('    - %s' % attr)

    # MorningStar Types
    types = set()
    FUNDs = set()
    STOCKs = set()
    ETFs = set()
    CEFs = set()
    NONEs = set()
    for quote, data in MFSData['MorningStarTypes'].items():
       types.add(data['Type'])
       if data['Type'] == 'FUND': FUNDs.add(quote)
       elif data['Type'] == 'STOCK': STOCKs.add(quote)
       elif data['Type'] == 'ETF': ETFs.add(quote)
       elif data['Type'] == 'CEF': CEFs.add(quote)
       else: NONEs.add(quote)
    logging.info('')
    logging.info('MorningStar Types:')
    logging.info('    %s' % types)
    
    logging.info('')
    logging.info('FUND  count: %s' % len(FUNDs))
    logging.info('STOCK count: %s' % len(STOCKs))
    logging.info('ETF   count: %s' % len(ETFs))
    logging.info('CEF   count: %s' % len(CEFs))
    logging.info('NONE  count: %s' % len(NONEs))

    types = set()
    funds = set()
    stocks = set()
    indexes = set()
    futures = set()
    for quote, data in MFSData['MarketWatchQuoteData'].items():
        if 'Type' in data:
            types.add(data['Type'])
            if data['Type'] == 'fund': funds.add(quote)
            elif data['Type'] == 'stock': stocks.add(quote)
            elif data['Type'] == 'index': indexes.add(quote)
            elif data['Type'] == 'future': futures.add(quote)
    logging.info('')
    logging.info('MarketWatchQuoteData Types:')
    logging.info('    %s' % types)

    logging.info('')
    logging.info('fund   count: %s' % len(funds))
    logging.info('stock  count: %s' % len(stocks))
    logging.info('index  count: %s' % len(indexes))
    logging.info('future count: %s' % len(futures))
    
    logging.info('')
    logging.info('FUND and fund   count: %s' % len(FUNDs.intersection(funds)))
    logging.info('FUND and stock  count: %s' % len(FUNDs.intersection(stocks)))
    logging.info('FUND and index  count: %s' % len(FUNDs.intersection(indexes)))
    logging.info('FUND and future count: %s' % len(FUNDs.intersection(futures)))

    logging.info('')
    logging.info('STOCK and fund   count: %s' % len(STOCKs.intersection(funds)))
    logging.info('STOCK and stock  count: %s' % len(STOCKs.intersection(stocks)))
    logging.info('STOCK and index  count: %s' % len(STOCKs.intersection(indexes)))
    logging.info('STOCK and future count: %s' % len(STOCKs.intersection(futures)))
    
    logging.info('')
    logging.info('ETF and fund   count: %s' % len(ETFs.intersection(funds)))
    logging.info('ETF and stock  count: %s' % len(ETFs.intersection(stocks)))
    logging.info('ETF and index  count: %s' % len(ETFs.intersection(indexes)))
    logging.info('ETF and future count: %s' % len(ETFs.intersection(futures)))
    
    logging.info('')
    logging.info('CEF and fund   count: %s' % len(CEFs.intersection(funds)))
    logging.info('CEF and stock  count: %s' % len(CEFs.intersection(stocks)))
    logging.info('CEF and index  count: %s' % len(CEFs.intersection(indexes)))
    logging.info('CEF and future count: %s' % len(CEFs.intersection(futures)))
    
    logging.info('')
    logging.info('NONE and fund   count: %s' % len(NONEs.intersection(funds)))
    logging.info('NONE and stock  count: %s' % len(NONEs.intersection(stocks)))
    logging.info('NONE and index  count: %s' % len(NONEs.intersection(indexes)))
    logging.info('NONE and future count: %s' % len(NONEs.intersection(futures)))

    # test ETF keywords in name with MS and MW type
    MSTypes = {}
    MWTypes = {}
    for quote, data in MFSData['MarketWatchQuotes'].items():
        if 'ETF' in data['Name'].upper():
            if quote in MFSData['MorningStarTypes']:
                MSType = MFSData['MorningStarTypes'][quote]['Type']
                if not MSType in MSTypes:
                    MSTypes[MSType] = 0
                MSTypes[MSType] += 1
            
            if quote in MFSData['MarketWatchQuoteData']:
                MWType = None
                if 'Type' in MFSData['MarketWatchQuoteData'][quote]:
                    MWType = MFSData['MarketWatchQuoteData'][quote]['Type']
                if not MWType in MWTypes:
                    MWTypes[MWType] = 0
                MWTypes[MWType] += 1
    logging.info('')
    logging.info('Types with key word ETF in name:')
    logging.info('Morning Star Types: %s' % MSTypes)
    logging.info('Market Watch Types: %s' % MWTypes)

    # test BOND keywords in name with MS and MW type
    MSTypes = {}
    MWTypes = {}
    for quote, data in MFSData['MarketWatchQuotes'].items():
        if 'BOND' in data['Name'].upper():
            if quote in MFSData['MorningStarTypes']:
                MSType = MFSData['MorningStarTypes'][quote]['Type']
                if not MSType in MSTypes:
                    MSTypes[MSType] = 0
                MSTypes[MSType] += 1
            
            if quote in MFSData['MarketWatchQuoteData']:
                MWType = None
                if 'Type' in MFSData['MarketWatchQuoteData'][quote]:
                    MWType = MFSData['MarketWatchQuoteData'][quote]['Type']
                if not MWType in MWTypes:
                    MWTypes[MWType] = 0
                MWTypes[MWType] += 1
    logging.info('')
    logging.info('Types with key word BOND in name:')
    logging.info('Morning Star Types: %s' % MSTypes)
    logging.info('Market Watch Types: %s' % MWTypes)

    # test INCOME keywords in name with MS and MW type
    MSTypes = {}
    MWTypes = {}
    for quote, data in MFSData['MarketWatchQuotes'].items():
        if 'INCOME' in data['Name'].upper():
            if quote in MFSData['MorningStarTypes']:
                MSType = MFSData['MorningStarTypes'][quote]['Type']
                if not MSType in MSTypes:
                    MSTypes[MSType] = 0
                MSTypes[MSType] += 1
            
            if quote in MFSData['MarketWatchQuoteData']:
                MWType = None
                if 'Type' in MFSData['MarketWatchQuoteData'][quote]:
                    MWType = MFSData['MarketWatchQuoteData'][quote]['Type']
                if not MWType in MWTypes:
                    MWTypes[MWType] = 0
                MWTypes[MWType] += 1
    logging.info('')
    logging.info('Types with key word INCOME in name:')
    logging.info('Morning Star Types: %s' % MSTypes)
    logging.info('Market Watch Types: %s' % MWTypes)
    
    # test EQUITY keywords in name with MS and MW type
    MSTypes = {}
    MWTypes = {}
    for quote, data in MFSData['MarketWatchQuotes'].items():
        if 'EQUITY' in data['Name'].upper():
            if quote in MFSData['MorningStarTypes']:
                MSType = MFSData['MorningStarTypes'][quote]['Type']
                if not MSType in MSTypes:
                    MSTypes[MSType] = 0
                MSTypes[MSType] += 1
            
            if quote in MFSData['MarketWatchQuoteData']:
                MWType = None
                if 'Type' in MFSData['MarketWatchQuoteData'][quote]:
                    MWType = MFSData['MarketWatchQuoteData'][quote]['Type']
                if not MWType in MWTypes:
                    MWTypes[MWType] = 0
                MWTypes[MWType] += 1
    logging.info('')
    logging.info('Types with key word EQUITY in name:')
    logging.info('Morning Star Types: %s' % MSTypes)
    logging.info('Market Watch Types: %s' % MWTypes)
    
    # test INDEX keywords in name with MS and MW type
    MSTypes = {}
    MWTypes = {}
    for quote, data in MFSData['MarketWatchQuotes'].items():
        if 'INDEX' in data['Name'].upper():
            if quote in MFSData['MorningStarTypes']:
                MSType = MFSData['MorningStarTypes'][quote]['Type']
                if not MSType in MSTypes:
                    MSTypes[MSType] = 0
                MSTypes[MSType] += 1
            
            if quote in MFSData['MarketWatchQuoteData']:
                MWType = None
                if 'Type' in MFSData['MarketWatchQuoteData'][quote]:
                    MWType = MFSData['MarketWatchQuoteData'][quote]['Type']
                if not MWType in MWTypes:
                    MWTypes[MWType] = 0
                MWTypes[MWType] += 1
    logging.info('')
    logging.info('Types with key word INDEX in name:')
    logging.info('Morning Star Types: %s' % MSTypes)
    logging.info('Market Watch Types: %s' % MWTypes)
    
    # test VALUE keywords in name with MS and MW type
    MSTypes = {}
    MWTypes = {}
    for quote, data in MFSData['MarketWatchQuotes'].items():
        if 'VALUE' in data['Name'].upper():
            if quote in MFSData['MorningStarTypes']:
                MSType = MFSData['MorningStarTypes'][quote]['Type']
                if not MSType in MSTypes:
                    MSTypes[MSType] = 0
                MSTypes[MSType] += 1
            
            if quote in MFSData['MarketWatchQuoteData']:
                MWType = None
                if 'Type' in MFSData['MarketWatchQuoteData'][quote]:
                    MWType = MFSData['MarketWatchQuoteData'][quote]['Type']
                if not MWType in MWTypes:
                    MWTypes[MWType] = 0
                MWTypes[MWType] += 1
    logging.info('')
    logging.info('Types with key word VALUE in name:')
    logging.info('Morning Star Types: %s' % MSTypes)
    logging.info('Market Watch Types: %s' % MWTypes)
    
    # asset allocation

    stocksVbondsMW = {}
    for quote, data in MFSData['MarketWatchHoldingsData'].items():
        if 'AssetAllocation' in data:
            bondsAmount = 0.0
            if 'Bonds' in data['AssetAllocation'] and data['AssetAllocation']['Bonds'] != None:
                bondsAmount = data['AssetAllocation']['Bonds'][0]
            bondsAmount = min(max(0.0, bondsAmount), 100.0)
            if bondsAmount < 0.0 or bondsAmount > 100.0: continue
            stocksAmount = 0.0
            if 'Stocks' in data['AssetAllocation'] and data['AssetAllocation']['Stocks'] != None:
                stocksAmount = data['AssetAllocation']['Stocks'][0]
            if stocksAmount < 0.0 or stocksAmount > 100.0: continue
            total = bondsAmount+stocksAmount
            if total == 0.0: continue
            stocksVbondsMW[quote] = (stocksAmount / total) * 100.0
    
    stocksVbondsYF = {}
    for quote, data in MFSData['YahooFinanceHoldingsData'].items():
        if 'AssetAllocation' in data:
            bondsAmount = 0.0
            if 'Bonds' in data['AssetAllocation'] and data['AssetAllocation']['Bonds'] != None:
                bondsAmount = data['AssetAllocation']['Bonds'][0]
            bondsAmount = min(max(0.0, bondsAmount), 100.0)
            if bondsAmount < 0.0 or bondsAmount > 100.0: continue
            stocksAmount = 0.0
            if 'Stocks' in data['AssetAllocation'] and data['AssetAllocation']['Stocks'] != None:
                stocksAmount = data['AssetAllocation']['Stocks'][0]
            if stocksAmount < 0.0 or stocksAmount > 100.0: continue
            total = bondsAmount+stocksAmount
            if total == 0.0: continue
            stocksVbondsYF[quote] = (stocksAmount / total) * 100.0


    logging.info('StocksBondsRatios in MW: %s' % len(stocksVbondsMW))
    logging.info('StocksBondsRatios in YF: %s' % len(stocksVbondsYF))

    # stocksVbondsBoth = set(stocksVbondsMW.keys()).intersection(set(stocksVbondsYF.keys()))
    # for quote in stocksVbondsBoth:
    #     diff = abs(stocksVbondsMW[quote] - stocksVbondsYF[quote])
    #     if diff > 10.0:
    #         logging.info('%s: %s' % ( quote, diff ) )
    
    # # analyze styles
    
    # for quote, data in MFSData['MorningStarQuoteData'].items():
    #     if 'Style' in data:
    #         if type(data['Style']) != dict:
    #             logging.info('%s: %s' % (quote, data['Style']))
    #     # elif 'CreditQuality' in data and 'InterestRateSensitivity' in data:
    #     elif 'CreditQuality' in data and data['CreditQuality'] != '—':
    #         # logging.info('%s: CreditQuality          : %s' % (quote, data['CreditQuality']))
    #         # logging.info('%s: InterestRateSensitivity: %s' % (quote, data['InterestRateSensitivity']))
    #         pass

    # MorningStarRating
    for quote, data in MFSData['MorningStarQuoteData'].items():
        morningStarRating = {}
        if quote in MFSData['MorningStarQuoteData'] and 'MorningStars' in MFSData['MorningStarQuoteData'][quote]:
            morningStarRating['MS'] = MFSData['MorningStarQuoteData'][quote]['MorningStars']
        if quote in MFSData['YahooFinanceQuoteData'] and 'MorningstarRating' in MFSData['YahooFinanceQuoteData'][quote]:
            morningStarRating['YF'] = MFSData['YahooFinanceQuoteData'][quote]['MorningstarRating']
        if len(morningStarRating) != 0:
            logging.info('%s: %s' % (quote, morningStarRating))

    # # categories check
    # categories = set()
    # for symbol, data in MFSData['MorningStarQuoteData'].items():
    #     if 'Category' in data and data['Category'] != None:
    #         categories.add(data['Category'])
    # MSQDCat = categories.copy()
    # categories = list(categories)
    # categories.sort()
    # logging.info('')
    # logging.info('MorningStarQuoteData Categories')
    # for category in categories:
    #     logging.info('    - %s' % category)

    # categories = set()
    # for symbol, data in MFSData['MarketWatchQuoteData'].items():
    #     if 'Category' in data and data['Category'] != None:
    #         categories.add(data['Category'])
    # categories = list(categories)
    # categories.sort()
    # logging.info('')
    # logging.info('MarketWatchQuoteData Categories')
    # for category in categories:
    #     logging.info('    - %s' % category)

    # categories = set()
    # for symbol, data in MFSData['YahooFinanceQuoteData'].items():
    #     if 'Category' in data and data['Category'] != None:
    #         categories.add(data['Category'])
    # YFQDCat = categories.copy()
    # categories = list(categories)
    # categories.sort()
    # logging.info('')
    # logging.info('YahooFinanceQuoteData Categories')
    # for category in categories:
    #     logging.info('    - %s' % category)

    # categories = YFQDCat.intersection(MSQDCat)
    # categories = list(categories)
    # categories.sort()
    # logging.info('')
    # logging.info('MorningStarQuoteData and YahooFinanceQuoteData same named Categories')
    # for category in categories:
    #     logging.info('    - %s' % category)

    # categories = set()
    # for symbol, data in MFSData['YFinanceTickerInfo'].items():
    #     if 'category' in data['Info'] and data['Info']['category'] != None:
    #         categories.add(data['Info']['category'])
    # YFTICat = categories.copy()
    # categories = list(categories)
    # categories.sort()
    # logging.info('')
    # logging.info('YFinanceTickerInfo Categories')
    # for category in categories:
    #     logging.info('    - %s' % category)
    
    # categories = YFQDCat.intersection(YFTICat)
    # categories = list(categories)
    # categories.sort()
    # logging.info('')
    # logging.info('YahooFinanceQuoteData and YFinanceTickerInfo same named Categories')
    # for category in categories:
    #     logging.info('    - %s' % category)
   
    # # sectors check

    # sectors = set()
    # for symbol, data in MFSData['MarketWatchQuotes'].items():
    #     sectors.add(data['Sector'])
    # YFQDCat = sectors.copy()
    # sectors = list(sectors)
    # sectors.sort()
    # logging.info('')
    # logging.info('MarketWatchQuotes Sectors')
    # for sector in sectors:
    #     logging.info('    - %s' % sector)

    # # YFinance info attributes

    # attributes = set()
    # for symbol, data in MFSData['YFinanceTickerInfo'].items():
    #     for attribute in data['Info'].keys():
    #         attributes.add(attribute)
    # attributes = list(attributes)
    # attributes.sort()
    # logging.info('')
    # logging.info('YFinanceTickerInfo Info attributes')
    # for attr in attributes:
    #     logging.info('    - %s' % attr)

    # # get style data
    
    # iTypes = set()
    # sTypes = set()
    # for symbol, data in MFSData['MorningStarQuoteData'].items():
    #     if 'Style' in data:
    #         for type, value in data['Style'].items():
    #             if type == 'Investment':  iTypes.add(value)
    #             if type == 'Style':  sTypes.add(value)
    # logging.info('')
    # logging.info('MorningStarQuoteData Investment Types')
    # for type  in iTypes:
    #     logging.info('    - %s' % type)
    # logging.info('MorningStarQuoteData Style Types')
    # for type  in sTypes:
    #     logging.info('    - %s' % type)

    # # types with styles
    # msTypes = set()
    # mwTypes = set()
    # for symbol, data in MFSData['MorningStarQuoteData'].items():
    #     if 'Style' in data:
    #         if symbol in MFSData['MorningStarTypes']:
    #             msTypes.add(MFSData['MorningStarTypes'][symbol]['Type'])
    #         if symbol in MFSData['MarketWatchQuoteData']:
    #             if 'Type' in MFSData['MarketWatchQuoteData'][symbol]:
    #                 mwTypes.add(MFSData['MarketWatchQuoteData'][symbol]['Type'])
    # logging.info('')
    # logging.info('MMorningStarTypes Types that have Style')
    # for type  in msTypes:
    #     logging.info('    - %s' % type)
    # logging.info('')
    # logging.info('MarketWatchQuoteData Types that have Style')
    # for type  in mwTypes:
    #     logging.info('    - %s' % type)

    # # check allocation types
    # msTypes = set()
    # mwTypes = set()
    # for symbol, data in MFSData['YahooFinanceQuoteData'].items():
    #     if 'Category' in data and data['Category'] != None:
    #         if data['Category'].startswith('Allocation'):
    #             msTypes.add(MFSData['MorningStarTypes'][symbol]['Type'])
    #             if "Type" in MFSData['MarketWatchQuoteData'][symbol]:
    #                 mwTypes.add(MFSData['MarketWatchQuoteData'][symbol]['Type'])
    # logging.info('')
    # logging.info('YahooFinanceQuoteData/Category that starts with "Allocation" are of types')
    # logging.info('    MorningStarTypes/Type    : %s' % msTypes)
    # logging.info('    MarketWatchQuoteData/Type: %s' % mwTypes)

    # # all exchanges
    # exchanges = set()
    # usExchanges = set()
    # for symbol, data in MFSData['MarketWatchQuotes'].items():
    #     exchanges.add(data['Exchange'])
    #     if data['Country'] == 'United States':
    #         usExchanges.add(data['Exchange'])
        
    # exchanges = list(exchanges)
    # exchanges.sort()
    # usExchanges = list(usExchanges)
    # usExchanges.sort()
    # logging.info('')
    # logging.info('MarketWatchQuotes/Exchange')
    # for exchange  in exchanges:
    #     if exchange in EI.Exchanges:
    #         logging.info('    - %s: %s' % (exchange, EI.Exchanges[exchange]))
    #     else:
    #         logging.info('    - %s: Undefined' % exchange)
    # logging.info('')
    # logging.info('MarketWatchQuotes/Exchange USA')
    # for exchange  in usExchanges:
    #     if exchange in EI.Exchanges:
    #         logging.info('    - %s: %s' % (exchange, EI.Exchanges[exchange]))
    #     else:
    #         logging.info('    - %s: Undefined' % exchange)

    # # find type combos
    # typeCombos = set()
    # for symbol in MFSData['Symbols']:
    #     typeCombo = ''
    #     if symbol in MFSData['MorningStarTypes'] and 'Type' in MFSData['MorningStarTypes'][symbol]:
    #         type = MFSData['MorningStarTypes'][symbol]['Type']
    #         if type == None:
    #             type = 'None'
    #         typeCombo = '%s/' % type
    #     else:
    #         typeCombo = '/'
    #     if symbol in MFSData['MarketWatchQuoteData'] and 'Type' in MFSData['MarketWatchQuoteData'][symbol]:
    #         type = MFSData['MarketWatchQuoteData'][symbol]['Type']
    #         if type == None:
    #             type = 'None'
    #         typeCombo += type
    #     typeCombos.add(typeCombo)
    # typeCombos = list(typeCombos)
    # typeCombos.sort()
    # logging.info('')
    # logging.info('Type combos MorningStarTypes/MarketWatchQuoteData')
    # for typeCombo  in typeCombos:
    #     logging.info('    - %s' % typeCombo)
