import pickle, logging, time, requests
from os.path import exists

from multiprocessing.dummy import Pool
from multiprocessing import cpu_count

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# create symbol block list for multiprocess
def getMultiSymbols(toDoSymbols, blockSize):
    sLength = len(toDoSymbols)
    multiSymbols = []
    while(sLength > 0):
        last = sLength
        if last > blockSize: last = blockSize

        multiSymbols.append(toDoSymbols[:last])

        toDoSymbols = toDoSymbols[last:]
        sLength = len(toDoSymbols)
    return multiSymbols

# quote scrape
def quoteScrape(symbol, messages):
    data = {}
    headers =  {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36'}
    url = 'https://www.morningstar.com/funds/xnas/%s/quote' % symbol
    statusCode = 500
    retries = -1
    while(statusCode == 500):
        retries = retries + 1
        r = requests.get(url, headers=headers)
        statusCode = r.status_code

    if retries > 0:
        # print('%s retries on' % (retries, symbol))
        messages.append('%s retries on' % (retries, symbol))
    
    # return None if quote page not found
    if statusCode == 404: return None
    
    soup = BeautifulSoup(r.text, 'html.parser')

    fundQuoteData = soup.find('div', class_='fund-quote__data')

    if fundQuoteData != None:
        # messages.append('%s: fundQuoteData found' % symbol)
        data['Name'] = symbol
        morningStars = soup.find('span', class_='mdc-security-header__star-rating')
        if morningStars != None:
            data['MorningStars'] = int(morningStars['title'].split()[0])

        expenseItem = fundQuoteData.find('span', text='Expense Ratio')
        if expenseItem != None:
            expenseItem = expenseItem.find_parent(class_='fund-quote__item')
            expenseItemText = expenseItem.find(class_='mdc-data-point mdc-data-point--number').text
            if expenseItemText != '—':
                data['ExpenseRatio'] = float(expenseItemText)

        initialItem = fundQuoteData.find('span', text='Min. Initial Investment')
        if initialItem != None:
            initialItem = initialItem.find_parent(class_='fund-quote__item')
            initialItemText = initialItem.find(class_='mdc-data-point mdc-data-point--number').text
            if initialItemText != '—':
                data['InitialInvestment'] = float(initialItemText.replace(',',''))

        category = fundQuoteData.find('span', text='Category')
        if category != None:
            category = category.find_parent(class_='fund-quote__item')
            categoryText = category.find(class_='mdc-data-point mdc-data-point--string').text
            if categoryText != '—':
                data['Category'] = categoryText
        
        styleBox = fundQuoteData.find(class_='mdc-data-point mdc-data-point--style-box')
        if styleBox != None:
            data['QuoteStyle'] = {}
            styleLabel = styleBox.find_parent(class_='fund-quote__item').find(class_='fund-quote__label')
            if ' / ' in styleLabel.text:
                styleNames = styleLabel.text.split(' / ')
                styleNames[0] = styleNames[0].replace(' ','')
                styleNames[1] = styleNames[1].replace(' ','')
                if styleBox.text != '—':
                    styleValues = styleBox.text.split(' / ')
                    styleValues[0] = styleValues[0].replace(' ','')
                    styleValues[1] = styleValues[1].replace(' ','')

                    data['QuoteStyle'][styleNames[0]] = styleValues[0]
                    data['QuoteStyle'][styleNames[1]] = styleValues[1]
                else:
                    data['QuoteStyle'][styleNames[0]] = None
                    data['QuoteStyle'][styleNames[1]] = None
            else:
                styleNames = styleLabel.text.split()
                if styleBox.text != '—':
                    styleValues = styleBox.text.split()
                    data['QuoteStyle'][styleNames[0]] = styleValues[0]
                    data['QuoteStyle'][styleNames[1]] = styleValues[1]
                else:
                    data['QuoteStyle'][styleNames[0]] = None
                    data['QuoteStyle'][styleNames[1]] = None
    else:
        messages.append('%s: fundQuoteData not found' % symbol)
        return None
    
    return data

# request and BS block handler
def blockProc(symbolsBlock):
    checkSymbols = {}
    messages = []
    for symbol in symbolsBlock:
        checkSymbols[symbol] = quoteScrape(symbol, messages)
    return [checkSymbols, messages]

def portfolioProc(symbol, driver, messages):
    if symbol == 'AACCX': return None
    data = {}
    url = 'https://www.morningstar.com/funds/xnas/%s/portfolio' % symbol
    # first check status code
    headers =  {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36'}
    # r = requests.get(url, headers=headers)
    statusCode = 500
    retries = -1
    while(statusCode == 500):
        retries = retries + 1
        r = requests.get(url, headers=headers)
        statusCode = r.status_code

    if retries > 0:
        # print('%s retries on' % (retries, symbol))
        messages.append('%s retries on: %s' % (retries, symbol))

    # return None if profile page not found
    if statusCode == 404: return None

    # logger.debug('Allocation scraping: %s' % url)
    tries = 3
    while tries > 0:
        try:
            driver.get(url)
            tries = 0
        except TimeoutException as inst:
            messages.append('TimeoutException: %s with: %s' % ((4 - tries), url))
            tries = tries -1
        except:
            messages.append('OtherException: %s' % (3 - tries))
            tries = tries -1
    if tries != 0: return None
    fundContent = driver.find_element(by=By.CLASS_NAME, value='fund__content')
    if fundContent == None: return data
    assetTableRow = fundContent.find_element(by=By.CLASS_NAME, value='sal-asset-allocation__assetTable')
    thead = assetTableRow.find_element(by=By.TAG_NAME, value='thead')
    valIndex = len(thead.find_elements(by=By.TAG_NAME, value='th')) - 1
    tbody = assetTableRow.find_element(by=By.TAG_NAME, value='tbody')
    trs = tbody.find_elements(by=By.TAG_NAME, value='tr')
    # print(trs[0].get_attribute('outerHTML'))
    trsIndex = 0
    totalAlloc = 0.0
    for allocation in ['usStock', 'nonUSStock', 'bonds', 'other', 'cash', 'notClassified']:
        tds = trs[trsIndex].find_elements(by=By.TAG_NAME, value='td')
        value = tds[valIndex].text
        if value == '—': continue
        data[allocation] = float(tds[valIndex].text)
        trsIndex = trsIndex + 1

    return data

def portfolioBlockProc(driversBlock):
    data = {}
    messages = []
    for symbol in driversBlock[1]:
        data[symbol] = portfolioProc(symbol, driversBlock[0], messages)

    return [data, messages]

# setup loogger
logger = logging.getLogger('testing_logger')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('MFScrapeDataFinal.log')
FORMAT = "%(asctime)-15s %(message)s"
fmt = logging.Formatter(FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(fmt)
logger.addHandler(handler)

# check if data file is already created
MFDataFile = 'MF_USA_DATA_FINAL.pickle'
MFData = {}
if exists(MFDataFile):
    with open(MFDataFile, 'rb') as f:
        MFData = pickle.load(f)
else:
    MFSymbolFile = 'MF_USA_SYMBOLS.pickle'
    with open(MFSymbolFile, 'rb') as f:
        symbols = pickle.load(f)
    for symbol in symbols:
        MFData[symbol] = None

# get None symbols to be scraped with quote data
toDoSymbols = []
for symbol, data in MFData.items():
    if data == None: toDoSymbols.append(symbol)
toDoSymbols.sort()

# create symbol block list for multiprocess
blockSize = 10
multiSymbols = getMultiSymbols(toDoSymbols, blockSize)

# setup processor count and pool
cpuCount = cpu_count() * 4
multiPool = Pool(cpuCount)

# setup multiprocess per block for quote scraping
blockStart = 0
# blockStart = 1408
sLength = ((len(multiSymbols)-1)*blockSize)+len(multiSymbols[-1])
while len(multiSymbols[blockStart:(blockStart+cpuCount)]) != 0:
    # print('%s %s' % (blockStart, (blockStart+cpuCount)))
    # print(multiSymbols[blockStart:(blockStart+cpuCount)])
    logger.debug('Symbol quote scrapes to do: %s' % (sLength-(blockStart*blockSize)))
    # print('Symbol checks to do: %s' % (sLength-(blockStart*blockSize)))
    
    multiResults = multiPool.map(blockProc, multiSymbols[blockStart:(blockStart+cpuCount)])

    for result in multiResults:
        for symbol, data in result[0].items():
            if data == None:
                MFData.pop(symbol)
            else:
                MFData[symbol] = data
        for message in result[1]:
            logger.debug(message)
    
    with open(MFDataFile, 'wb') as f:
        pickle.dump(MFData, f, protocol=pickle.HIGHEST_PROTOCOL)

    blockStart = blockStart + cpuCount

# get read symbols to be scraped with portfolio data
toDoSymbols = []
for symbol, data in MFData.items():
    if (type(data) == dict) and not ('Allocation' in data):
        toDoSymbols.append(symbol)
toDoSymbols.sort()
totalLength = len(toDoSymbols)
if totalLength == 0: exit(0)

# create and start webdrivers for profile scalping
poolSize = 4
drivers = []
for x in range(poolSize):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.implicitly_wait(25)
    driver.set_page_load_timeout(30)
    drivers.append(driver)

# create multi blocks for multiprocess webdrivers
multiSymbols = getMultiSymbols(toDoSymbols, 5)
poolBlocks = []
driversBlocks = []
driverIndex = 0
for symbolsBlock in multiSymbols:
    if driverIndex < poolSize:
        poolBlocks.append([drivers[driverIndex], symbolsBlock])
        driverIndex = driverIndex + 1
        continue

    driversBlocks.append(poolBlocks)
    driverIndex = 0
    poolBlocks = []
    poolBlocks.append([drivers[driverIndex], symbolsBlock])
    driverIndex = driverIndex + 1
if len(poolBlocks) > 0: driversBlocks.append(poolBlocks)

# run multiproc on profile blocks for webdrivers
for driversBlock in driversBlocks:
    logger.debug('Symbol portfolio scrapes to do: %s' % totalLength)
    portfolioResults = multiPool.map(portfolioBlockProc, driversBlock)
    for result in portfolioResults:
        for symbol, data in result[0].items():
            if data == None: continue
            MFData[symbol]['Allocation'] = data
        for message in result[1]:
            logger.debug(message)

    with open(MFDataFile, 'wb') as f:
        pickle.dump(MFData, f, protocol=pickle.HIGHEST_PROTOCOL)

for driver in drivers:
    driver.quit()
