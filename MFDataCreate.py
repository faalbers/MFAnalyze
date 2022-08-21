import DataScrape as DS
import logging

if __name__ == "__main__":
    scrapedFileName = 'MF_DATA_SCRAPED'
    dataFileName = 'MF_DATA'

    DS.setupLogging('MFDataCreate.log', timed=False, new=True)

    MFSData = DS.getData(scrapedFileName)
    MFData = {}
    MFData['Quotes'] = {}

    # get all funds quotes and sort them
    quotes = []
    for quote in MFSData['MorningStarQuoteData'].keys():
        quotes.append(quote)
    quotes.sort()
    for quote in quotes:
        MFData['Quotes'][quote] = {}

    values = set()
    for quote in quotes:
        MFData['Quotes'][quote] = {}
        quoteSplits = quote.split(':')

        # create data structure
        fund = {
            'Symbol': None,
            'Name': None,
            'Country': None,
            'CISO': None,
            'Type': None
        }
        MFData['Quotes'][quote]['Fund'] = fund

        exchange = {
            'MIC': None,
            'Name': None,
            'CISO': None,
            'Country': None
        }
        MFData['Quotes'][quote]['Exchange'] = exchange
        
        data = {
            'MinInvestment': None,
            'Yield': None,
            'Rating': None,
            'ETradeAvailbility': None,
            'MorningStarRating': None,
            'Expense': {
                'NetExpenseRatio': None,
                'TotalExpenseRatio': None,
                'ExpenseRatio': None,
                'AdjExpenseRatio': None
            },
            'Bonds': {
                'CreditQuality': None,
                'InterestRateSensitivity': None
            },
            'Stocks': {
                'Cap': None,
                'Style': None
            },
            'AssetAllocation': {
                'Stocks': None,
                'Bonds': None,
                'Convertible': None,
                'Other': None,
                'Cash': None,
                'StocksBondsRatio': None
            }
        }
        MFData['Quotes'][quote]['Data'] = data


        fund['Symbol'] = quoteSplits[0]
        fund['Name'] = MFSData['MarketWatchQuotes'][quote]['Name']
        for fCISO, fcountry in MFSData['CISOs'].items():
            if 'Country' in MFSData['MarketWatchQuotes'][quote]:
                if fcountry.startswith(MFSData['MarketWatchQuotes'][quote]['Country']):
                    fund['Country'] = fcountry
                    fund['CISO'] = fCISO

        exchange['MIC'] = quoteSplits[1]
        exchange['Name'] = MFSData['MICs'][quoteSplits[1]]['Name']
        exchange['CISO'] = list(MFSData['MICToCISO'][quoteSplits[1]])[0]
        exchange['Country'] = MFSData['CISOs'][exchange['CISO']]

        # handle MarketWatchQuoteData
        if 'MarketWatchQuoteData' in MFSData:
            qdata = MFSData['MarketWatchQuoteData'][quote]
            if 'MinInvestment' in qdata and qdata['MinInvestment'] != None:
                data['MinInvestment'] = qdata['MinInvestment'][0]

            if 'Yield' in qdata and qdata['Yield'] != None:
                data['Yield'] = qdata['Yield'][0]
            
            if 'NetExpenseRatio' in qdata and qdata['NetExpenseRatio'] != None:
                data['Expense']['NetExpenseRatio'] = qdata['NetExpenseRatio'][0]
            
            if 'TotalExpenseRatio' in qdata and qdata['TotalExpenseRatio'] != None:
                data['Expense']['TotalExpenseRatio'] = qdata['TotalExpenseRatio'][0]

        # handle MorningStarQuoteData
        if 'MorningStarQuoteData' in MFSData:
            qdata = MFSData['MorningStarQuoteData'][quote]
            if 'CreditQuality' in qdata:
                data['Bonds']['CreditQuality'] = qdata['CreditQuality']
                data['Bonds']['InterestRateSensitivity'] = qdata['InterestRateSensitivity']

            if 'InvestmentStyle' in qdata and qdata['InvestmentStyle'] != '—':
                splits = qdata['InvestmentStyle'].split()
                data['Stocks']['Cap'] = splits[0]
                data['Stocks']['Style'] = splits[1]

            if 'Rating' in qdata:
                data['MorningStarRating'] = qdata['Rating']

            if 'FundType' in qdata:
                fund['Type'] = qdata['FundType']

            if 'ExpenseRatio' in qdata and qdata['ExpenseRatio'] != '—':
                if type(qdata['ExpenseRatio']) == list:
                    data['Expense']['ExpenseRatio'] = qdata['ExpenseRatio'][0]
                else:
                    data['Expense']['ExpenseRatio'] = qdata['ExpenseRatio']
            
            if 'AdjExpenseRatio' in qdata and qdata['AdjExpenseRatio'] != '—':
                if type(qdata['AdjExpenseRatio']) == list:
                    data['Expense']['AdjExpenseRatio'] = qdata['AdjExpenseRatio'][0]
                else:
                    data['Expense']['AdjExpenseRatio'] = qdata['AdjExpenseRatio']

        # # get name and deduct strategies from name
        # data['Name'] = MFSData['MarketWatchQuotes'][quote]['Name']
        # data['Strategies'] = set()
        # if 'ETF' in data['Name'].upper():
        #     data['Strategies'].add('ETF')
        # if 'BOND' in data['Name'].upper():
        #     data['Strategies'].add('BOND')
        # if 'INCOME' in data['Name'].upper():
        #     data['Strategies'].add('INCOME')
        # if 'GROWTH' in data['Name'].upper():
        #     data['Strategies'].add('GROWTH')
        # if 'VALUE' in data['Name'].upper():
        #     data['Strategies'].add('VALUE')
        # if 'EQUITY' in data['Name'].upper():
        #     data['Strategies'].add('EQUITY')
        # if 'INDEX' in data['Name'].upper():
        #     data['Strategies'].add('INDEX')
        # if 'TARGET' in data['Name'].upper():
        #     data['Strategies'].add('TARGET')
        # if 'GLOBAL' in data['Name'].upper():
        #     data['Strategies'].add('GLOBAL')
        # if 'INTERNATIONAL' in data['Name'].upper() or 'INTL' in data['Name'].upper():
        #     data['Strategies'].add('INTERNATIONAL')
        # if 'EMERGING' in data['Name'].upper():
        #     data['Strategies'].add('EMERGING')
        # if 'BALANCED' in data['Name'].upper() and not 'RISK' in data['Name'].upper():
        #     data['Strategies'].add('BALANCED')
        # if 'BALANCED' in data['Name'].upper() and 'RISK' in data['Name'].upper():
        #     data['Strategies'].add('BALANCEDRISK')

        # handle MarketWatchHoldingsData
        if 'MarketWatchHoldingsData' in MFSData:
            qdata = MFSData['MarketWatchHoldingsData'][quote]
            # Marketwatch Allocation data seemed more reliable then YahooFinance
            # because totals where closer to 100%
            if 'AssetAllocation' in qdata:
                adata = qdata['AssetAllocation']
                if 'Stocks' in adata:
                    data['AssetAllocation']['Stocks'] = adata['Stocks'][0]
                if 'Bonds' in adata:
                    data['AssetAllocation']['Bonds'] = adata['Bonds'][0]
                if 'Convertible' in adata:
                    data['AssetAllocation']['Convertible'] = adata['Convertible'][0]
                if 'Other' in adata:
                    data['AssetAllocation']['Other'] = adata['Other'][0]
                if 'Cash' in adata:
                    data['AssetAllocation']['Cash'] = adata['Cash'][0]

                stocksAmount =  0.0
                if data['AssetAllocation']['Stocks'] != None:
                    stocksAmount = data['AssetAllocation']['Stocks']
                if stocksAmount >= 0.0 and stocksAmount <= 100.0:
                    bondsAmount =  0.0
                    if data['AssetAllocation']['Bonds'] != None:
                        bondsAmount = data['AssetAllocation']['Bonds']
                    if bondsAmount >= 0.0 and bondsAmount <= 100.0:
                        total = bondsAmount+stocksAmount
                        if total != 0.0:
                            data['AssetAllocation']['StocksBondsRatio'] = (stocksAmount / total) * 100.0
        
        # etrade data
        if 'ETradeQuoteData' in MFSData:
            qdata = MFSData['ETradeQuoteData'][quote]
            if 'MutualFund' in qdata:
                if qdata['MutualFund']['availability'] == 'Open to New Buy and Sell':
                    data['ETradeAvailbility'] = 'Open'
                else:
                    data['ETradeAvailbility'] = 'Close'
        
    print(values)

    # log quotes
    for quote, data in MFData['Quotes'].items():
        logging.info(quote)
        for attribute, value in data.items():
            logging.info('%s: %s' % (attribute,value))
        logging.info('')
    
    if not DS.saveData(MFData, dataFileName):
        logging.info('%s: Stop saving data and exit program' % dataFileName)
        exit(0)
