import DataScrape as DS
import logging

if __name__ == "__main__":
    DS.setupLogging('AmyTest.csv', timed=False, new=True)
    dataFileName = 'MF_DATA'
    MFData = DS.getData(dataFileName)

    logging.info('Symbol, Name, Yield, MorningStarRating, ExpenseRatio, ETrade')

    yields = {}
    for quote, data in MFData.items():
        countryCode = quote.split(':')[2]
        if data['Yield'] != None and countryCode == 'US' and data['MorningStarRating'] != None:
            yields[quote] = data['Yield']
    values = set()
    for quote, value in yields.items():
        values.add(value)
    values = list(values)
    values.sort(reverse=True)
    for value in values:
        for quote, yvalue in yields.items():
            if yvalue == value:
                symbol = quote.split(':')[0]
                name = MFData[quote]['Name']
                morningstar = MFData[quote]['MorningStarRating']
                expense = MFData[quote]['ExpenseRatio']
                etrade = 'N/A'
                if MFData[quote]['ETradeAvailable'] != None:
                    etrade = MFData[quote]['ETradeAvailable']
                logging.info('%s, %s, %s, %s, %s, %s' % (symbol, name, yvalue, morningstar, expense, etrade))



