import DataScrape as DS
import logging

# main program
if __name__ == "__main__":
    dataFileName = 'MF_DATA_SCRAPED'
    MFData = DS.getData(dataFileName)
    DS.setupLogging('%s_REPORT.log' % dataFileName, timed=False, new=True)

    logging.info('Data file   : %s' % dataFileName)

    logging.info('Quote count  : %s' % len(MFData['Quotes']))
    logging.info('Country count: %s' % len(MFData['CountryCodes']))

    subNames = list(MFData.keys())
    subNames.remove('Quotes')
    subNames.remove('CountryCodes')
    subNames.sort()
    for subName in subNames:
        logging.info('')
        logging.info('Sub data: %s' % subName)
        quotes = list(MFData[subName].keys())
        logging.info('    Symbol count: %s' % len(quotes))
        scrapeTimes = []
        for quote in quotes:
            scrapeTimes.append(MFData[subName][quote]['ScrapeTag'])
        logging.info('    Scrape Start: %s' % min(scrapeTimes))
        logging.info('    Scrape End  : %s' % max(scrapeTimes))

