import DataScrape as DS
import yfinance as yf
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
import logging, time, os

def timingProc(sleepTime):
    time.sleep(sleepTime)
    logging.info("slept %s seconds" % sleepTime)

def testProc(symbol):
    ticker = yf.Ticker(symbol)
    return ticker.info

def testYFDataSingle():
    logging.info('Start YFinance Query Test')
    queryCount = 1
    queryInterval = 0

    while True:
        # print ('Test: %s' % (queryCount + queryInterval))
        # time.sleep(1)
        data = testProc('VITAX')
        if len(data) < 4:
            logging.info('EMPTY on test: %s' % (queryCount + queryInterval))
            logging.info('Waiting for 1 minute ...')
            time.sleep(60)

        queryInterval += 1
        if queryInterval == 10:
            queryCount += queryInterval
            queryInterval = 0
            logging.info('Tests so far: %s' % (queryCount-1))

def testMultiTiming(processes):
    logging.info('Start testMultiTiming with process count: %s' % processes)
    timeList = list(range(1, processes+1))
    multiPool = Pool(processes)
    results = multiPool.map(timingProc, timeList)
    logging.info('End testMultiTiming with process count: %s' % processes)

def final():
    dataFileName = 'MF_DATA'
    yfFileName = 'YF_DATA'
    MFData = DS.getData(dataFileName)
    YFData = DS.getData(yfFileName)

    todoSymbols = MFData['Symbols']
    doneSymbols = set(YFData.keys())
    todoSymbols = list(todoSymbols.difference(doneSymbols))
    todoSymbols.sort()

    processes = 4
    sleepStepTime = 30
    multiPool = Pool(processes)

    sTotal = len(todoSymbols)
    for block in DS.makeMultiBlocks(todoSymbols, processes):
        logging.info('symbols to scrape quote data with Yahoo Finance: %s' % sTotal)

        results = []
        retry = True
        while retry:
            results = multiPool.map(testProc, block)
            retry = False

            # test if locked
            totalSleep = 0
            while len(testProc('VITAX')) < 4:
                retry = True
                time.sleep(sleepStepTime)
                totalSleep += sleepStepTime
                logging.info('waiting for %s seconds to unblock' % totalSleep)
            if retry:
                logging.info('retry same symbols to scrape quote data with Yahoo Finance')

        sIndex = 0
        for data in results:
            symbol = block[sIndex]
            if symbol == 'VITAX':
                logging.info(symbol)
                logging.info(data)
            YFData[symbol] = data
            sIndex += 1

        DS.saveData(YFData, yfFileName)
        
        sTotal = sTotal - len(block)

def testYFDataMulti(processes):
    logging.info('YFinance Multi Query Test with process count: %s' % processes)
    multiPool = Pool(processes)

    queryCount = 0
    lastQCount = 0
    startTime = time.time()
    currentTime = 1.0
    while True:
        symbols = ['VITAX'] * processes
        if lastQCount > 9:
            speed = (3600.0/currentTime) * queryCount
            logging.info('Tests so far: %s during: %.f seconds at per hour: %s' % (queryCount, currentTime, speed))
            lastQCount = 0
        results = multiPool.map(testProc, symbols)
        queryCount += processes
        lastQCount += processes
        emptyCount = 0
        for data in results:
            if len(data) < 4: emptyCount += 1
        if emptyCount > 0:
            logging.info('empty count: %s' % emptyCount)
            totalSleep = 0
            while len(testProc('VITAX')) < 4:
                time.sleep(30)
                totalSleep += 30
                logging.info('slept for %s' % totalSleep)
        currentTime = time.time() - startTime

def testYFDataA():
    todoSymbols = ['VITAX'] * 20000

    sTotal = len(todoSymbols)
    cpuCount = cpu_count() * 4
    multiPool = Pool(cpuCount)
    # for block in DS.makeMultiBlocks(todoSymbols, 400):
    for block in DS.makeMultiBlocks(todoSymbols, 100):
        logging.info('testing symbols: %s' % sTotal)
        testData = {}
        totalSleepTime = 0
        while len(testData) < 4:
            testData = testProc('VITAX')
            if len(testData) < 4:
                time.sleep(60)
                totalSleepTime += 60
                logging.info('sleep time: %s' % totalSleepTime)
        results = multiPool.map(testProc, block)

        emptyCount = 0
        for data in results:
            if len(data) < 4: emptyCount += 1

        logging.info('empty count: %s' % emptyCount)
    
        sTotal = sTotal - len(block)

def getYFData():
    dataFileName = 'MF_DATA'
    yfFileName = 'YF_DATA'
    MFData = DS.getData(dataFileName)
    YFData = DS.getData(yfFileName)
    
    todoSymbols = list(MFData['Symbols'])
    todoSymbols.sort()
    # todoSymbols = todoSymbols[35200:]
    todoSymbols = todoSymbols[35487:]
    print(todoSymbols[0])

    sTotal = len(todoSymbols)
    cpuCount = cpu_count() * 4
    multiPool = Pool(cpuCount)
    # for block in DS.makeMultiBlocks(todoSymbols, 400):
    for block in DS.makeMultiBlocks(todoSymbols, 1):
        print(block)
        logging.info('symbols to scrape quote data with Yahoo Finance: %s' % sTotal)
        results = multiPool.map(testProc, block)
        print(results)

        sIndex = 0
        for data in results:
            symbol = block[sIndex]
            if symbol == 'VITAX':
                logging.info(symbol)
                logging.info(data)
            YFData[symbol] = data
            sIndex += 1

        # DS.saveData(YFData, yfFileName)
        
        sTotal = sTotal - len(block)
        break

# main program
if __name__ == "__main__":
    # dataFileName = 'MF_DATA'
    yfFileName = 'YF_DATA'
    
    DS.setupLogging('MFYFinance.log', timed=True, new=False)

    # testYFDataB()
    # print(testProc('VITAX'))
    # testYFDataMulti(4)
    # testMultiTiming(10)
    # final()

    YFData = DS.getData(yfFileName)

    emptyCount = 0
    for symbol, data in YFData.items():
        if len(data) < 4:
            emptyCount += 1

    print(len(YFData))
    print(emptyCount)
