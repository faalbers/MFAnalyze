import DataScrape as DS
import logging

if __name__ == "__main__":
    scrapedFileName = 'MF_DATA_SCRAPED'
    dataFileName = 'MF_DATA'

    DS.setupLogging('MFDataCreate.log', timed=False, new=True)

    MFSData = DS.getData(scrapedFileName)
    MFData = DS.getData(dataFileName)

    # get all funds and add them alphabetically
    quotes = []
    for quote in MFSData['Quotes']:
        quotes.append(quote)
    quotes.sort()
    for quote in quotes:
        MFData[quote] = {}
    
    for quote, data in MFData.items():
        # vars that could empty or None
        allocations = None
        # stocksBondsRatio = None
        bondStyle = None
        stockStyle = None
        morningStarRating = None
        minInvestment = None
        Yield = None
        expenseRatio = None
        etradeAvailable = None

        # get name and deduct strategies from name
        data['Name'] = MFSData['MarketWatchQuotes'][quote]['Name']
        data['Strategies'] = set()
        if 'ETF' in data['Name'].upper():
            data['Strategies'].add('ETF')
        if 'BOND' in data['Name'].upper():
            data['Strategies'].add('BOND')
        if 'INCOME' in data['Name'].upper():
            data['Strategies'].add('INCOME')
        if 'GROWTH' in data['Name'].upper():
            data['Strategies'].add('GROWTH')
        if 'VALUE' in data['Name'].upper():
            data['Strategies'].add('VALUE')
        if 'EQUITY' in data['Name'].upper():
            data['Strategies'].add('EQUITY')
        if 'INDEX' in data['Name'].upper():
            data['Strategies'].add('INDEX')
        if 'TARGET' in data['Name'].upper():
            data['Strategies'].add('TARGET')
        if 'GLOBAL' in data['Name'].upper():
            data['Strategies'].add('GLOBAL')
        if 'INTERNATIONAL' in data['Name'].upper() or 'INTL' in data['Name'].upper():
            data['Strategies'].add('INTERNATIONAL')
        if 'EMERGING' in data['Name'].upper():
            data['Strategies'].add('EMERGING')
        if 'BALANCED' in data['Name'].upper() and not 'RISK' in data['Name'].upper():
            data['Strategies'].add('BALANCED')
        if 'BALANCED' in data['Name'].upper() and 'RISK' in data['Name'].upper():
            data['Strategies'].add('BALANCEDRISK')

        # get MarketWatchQuotes data
        data['Country'] = MFSData['MarketWatchQuotes'][quote]['Country']

        # handle MarketWatchQuoteData
        if quote in MFSData['MarketWatchQuoteData']:
            qdata = MFSData['MarketWatchQuoteData'][quote]
            
            if 'MinInvestment' in qdata and qdata['MinInvestment'] != None:
                minInvestment = qdata['MinInvestment'][0]

            if 'Yield' in qdata and qdata['Yield'] != None:
                Yield = qdata['Yield'][0]
            
            if 'NetExpenseRatio' in qdata  and qdata['NetExpenseRatio'] != None and expenseRatio == None:
                expenseRatio = qdata['NetExpenseRatio'][0]
        
        # handle MorningStarQuoteData
        if quote in MFSData['MorningStarQuoteData']:
            qdata = MFSData['MorningStarQuoteData'][quote]
            
            if 'CreditQuality' in qdata and qdata['CreditQuality'] != '—':
                bondStyle = {}
                bondStyle['CreditQuality'] = qdata['CreditQuality'].split(' / ')[0]
                bondStyle['InterestRateSensitivity'] = qdata['InterestRateSensitivity'].split(' / ')[1]
            
            if 'Style' in qdata and qdata['Style']['Investment'] != '—':
                stockStyle = {}
                stockStyle['Cap'] = qdata['Style']['Investment']
                stockStyle['Style'] = qdata['Style']['Style']
            
            if 'MorningStars' in qdata:
                morningStarRating = qdata['MorningStars']

            if 'ExpenseRatio' in qdata and qdata['AdjExpenseRatio'] != '—':
                if type(qdata['ExpenseRatio']) == list:
                    expenseRatio = qdata['ExpenseRatio'][0]
                else:
                    expenseRatio = qdata['ExpenseRatio']

        # handle MarketWatchHoldingsData
        if quote in MFSData['MarketWatchHoldingsData']:
            qdata = MFSData['MarketWatchHoldingsData'][quote]

            # Marketwatch Allocation data seemed more reliable then YahooFinance
            # because totals where closer to 100%
            if 'AssetAllocation' in qdata:
                qdata = qdata['AssetAllocation']
                stocks = 0.0
                bonds = 0.0
                convertible = 0.0
                preferred = 0.0
                other = 0.0
                cash = 0.0
                if 'Stocks' in qdata:
                    stocks = qdata['Stocks'][0]
                if 'Bonds' in qdata:
                    bonds = qdata['Bonds'][0]
                if 'Convertible' in qdata:
                    convertible = qdata['Convertible'][0]
                if 'Other' in qdata:
                    other = qdata['Other'][0]
                if 'Cash' in qdata:
                    cash = qdata['Cash'][0]
                all = stocks + bonds + convertible + preferred + other + cash
                if all > 0.0:
                    allocations = {}
                    allocations['Stocks'] = stocks
                    allocations['Bonds'] = bonds
                    allocations['Convertible'] = convertible
                    allocations['Other'] = other
                    allocations['Cash'] = cash

                # stocksAmount = 0.0
                # if 'Stocks' in qdata and qdata['Stocks'] != None:
                #     stocksAmount = qdata['Stocks'][0]
                # if stocksAmount >= 0.0 and stocksAmount <= 100.0:
                #     bondsAmount = 0.0
                #     if 'Bonds' in qdata and qdata['Bonds'] != None:
                #         bondsAmount = qdata['Bonds'][0]
                #     if bondsAmount >= 0.0 and bondsAmount <= 100.0:
                #         total = bondsAmount+stocksAmount
                #         if total != 0.0:
                #             stocksBondsRatio = (stocksAmount / total) * 100.0
        
        # # Add BALANCED strategy
        # # I might reconsider this
        # if stocksBondsRatio != None:
        #     if stocksBondsRatio > 25.0 and stocksBondsRatio < 75.0:
        #         data['Strategies'].add('BALANCED')

        # etrade data
        if quote in MFSData['ETradeQuoteData']:
            qdata = MFSData['ETradeQuoteData'][quote]
            if 'MutualFund' in qdata and qdata['MutualFund']['availability'] == 'Open to New Buy and Sell':
                etradeAvailable = 'Open'
            else:
                etradeAvailable = 'Close'
        
        # add found vars
        data['Allocations'] = allocations
        # data['StocksBondsRatio'] = stocksBondsRatio
        data['BondStyle'] = bondStyle
        data['StockStyle'] = stockStyle
        data['MorningStarRating'] = morningStarRating
        data['MinInvestment'] = minInvestment
        data['Yield'] = Yield
        data['ExpenseRatio'] = expenseRatio
        data['ETradeAvailable'] = etradeAvailable

    # log quotes
    for quote, data in MFData.items():
        logging.info(quote)
        for attribute, value in data.items():
            logging.info('%s: %s' % (attribute,value))
        logging.info('')
    
    if not DS.saveData(MFData, dataFileName):
        logging.info('%s: Stop saving data and exit program' % dataFileName)
        exit(0)
