import requests, logging, math, pickle, time
from os.path import exists
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

def setLogger(filename, new=True, timed=False):
    filemode = 'a'
    if new: filemode = 'w'
    formatStr = '%(message)s'
    datefmtStr = '%m/%d/%Y %I:%M:%S %p'
    if timed:
        formatStr = '%(asctime)s: %(message)s'
    logging.basicConfig(
        filename=filename, encoding='utf-8', level=logging.INFO, filemode=filemode,
        format=formatStr, datefmt=datefmtStr
        )

def getData(fileName):
    dataFile = '%s.pickle' % fileName
    data = {}
    if exists(dataFile):
        with open(dataFile, 'rb') as f:
            data = pickle.load(f)
    return data

def saveData(data, fileName):
    dataFile = '%s.pickle' % fileName
    with open(dataFile, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

# chop data list into block lists
def makeMultiBlocks(data, blockSize):
    multiBlocks = []
    dataCopy = data.copy()
    dataSize = len(dataCopy)
    while(dataSize > 0):
        last = dataSize
        if last > blockSize: last = blockSize

        multiBlocks.append(dataCopy[:last])

        dataCopy = dataCopy[last:]
        dataSize = len(dataCopy)

    return multiBlocks

def getRequest(url, maxRetries=10):
    headers =  {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36'}
    statusCode = 500
    retries = -1
    while(retries < maxRetries and statusCode == 500):
        retries = retries + 1
        r = requests.get(url, headers=headers)
        statusCode = r.status_code
    return r    

def getStatusCode(url, maxRetries=10):
    headers =  {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36'}
    statusCode = 500
    retries = -1
    while(retries < maxRetries and statusCode == 500):
        retries = retries + 1
        r = requests.head(url, headers=headers)
        statusCode = r.status_code
    return statusCode    

# def getStatusCodes(urls):
#     cpuCount = cpu_count() * 4
#     multiPool = Pool(cpuCount)
#     return multiPool.map(getStatusCode, urls)

# def urlScrapeProc(url, scrapeProc):
#     headers =  {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36'}
#     statusCode = 500
#     retries = -1
#     while(statusCode == 500):
#         retries = retries + 1
#         r = requests.get(url, headers=headers)
#         statusCode = r.status_code

#     if retries > 0:
#         logger.info('%s statusCodeProc retries on %s' % (retries, url))
    
#     if statusCode == 404: return None

#     data = scrapeProc(r)

#     return data
    
# def urlScrape(urls, scrapeProc):
#     data = []

#     poolArgs = []
#     for url in urls:
#         poolArgs.append([url, scrapeProc])
    
#     cpuCount = cpu_count() * 4
#     multiPool = Pool(cpuCount)
#     results = multiPool.map(urlScrapeProc, urls)

#     return results

# def multiScrapeStatusCode(poolVariables, results, statusCode, indices=False):
#     pVarsSC = []
#     scIndices = []
#     scIndex = 0
#     for result in results[0]:
#         if result == statusCode:
#             pVarsSC.append(poolVariables[scIndex])
#             scIndices.append(scIndex)
#         scIndex = scIndex + 1
#     if indices: return scIndices
#     return pVarsSC

# def multiScrapeOK(poolVariables, results, indices=False):
#     return multiScrapeStatusCode(poolVariables, results, 200, indices)

# def multiScrapeNotFound(poolVariables, results):
#     return multiScrapeStatusCode(poolVariables, results, 404, indices)

# def multiScrapeBlocked(poolVariables, results, indices=False):
#     return multiScrapeStatusCode(poolVariables, results, 403, indices)

# def multiScrapeError(poolVariables, results, indices=False):
#     return multiScrapeStatusCode(poolVariables, results, 500, indices)

def unblockSleep(poolVariable, scrapeProc):
    sleepTime = 0
    timeIncrement = 5
    blocked = True
    while blocked:
        data = scrapeProc(poolVariable)
        if data[0] != 403: break
        sleepTime += timeIncrement
        time.sleep(timeIncrement)
    return sleepTime

def multiScrape(poolVariables, scrapeProc):
    data = [ [None] * len(poolVariables), [None] * len(poolVariables)]
    pVarIndices = list(range(len(poolVariables)))
    cpuCount = cpu_count() * 4
    multiPool = Pool(cpuCount)
    while len(pVarIndices) != 0:
        pVarsRetry = [poolVariables[i] for i in pVarIndices]
        results = multiPool.map(scrapeProc, pVarsRetry)
        rStatusCodes = []
        rData = []
        for result in results:
            rStatusCodes.append(result[0])
            rData.append(result[1])
        
        pVarIndex = 0
        pVarBlockedIndices = []
        for statusCode in rStatusCodes:
            if statusCode == 403:
                pVarBlockedIndices.append(pVarIndices[pVarIndex])
            else:
                data[0][pVarIndices[pVarIndex]] = statusCode
                data[1][pVarIndices[pVarIndex]] = rData[pVarIndex]
            pVarIndex += 1
        
        if len(pVarBlockedIndices) == 0:
            pVarIndices = []
        else:
            pVarIndices = pVarBlockedIndices
            logger.info('retrying %s blocked attempts' % len(pVarBlockedIndices))
            sleepTime = unblockSleep(pVarsRetry[0], scrapeProc)
            logger.info('slept %s seconds' % sleepTime)
    return data

# def startWebdrivers(driversCount):
#     drivers = []
#     for x in range(driversCount):
#         driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
#         driver.implicitly_wait(25)
#         driver.set_page_load_timeout(30)
#         drivers.append(driver)
#     return drivers

# def quitWebDrivers(webDrivers):
#     for driver in webDrivers:
#         driver.quit()

# def webdriverScrape(data):
#     # poolVariables, scrapeProc, driver
#     results = []
#     for item in data[0]:
#         results.append(data[1](item, data[2]))
#     return results

# def multiWebdriversScrape(poolVariables, scrapeProc, webrivers):
#     blockSize = int(math.ceil(len(poolVariables)/len(webrivers)))
#     multiBlocks = []
#     dIndex = 0
#     for blockVariables in makeMultiBlocks(poolVariables, blockSize):
#         multiBlocks.append([blockVariables, scrapeProc, webrivers[dIndex]])
#         dIndex = dIndex + 1

#     cpuCount = cpu_count() * 4
#     multiPool = Pool(cpuCount)
#     results = multiPool.map(webdriverScrape, multiBlocks)

#     return results

# def makeUrls(mainUrl, keys):
#     urls = []
#     for key in keys:
#         urls.append(mainUrl % key)
#     return urls

