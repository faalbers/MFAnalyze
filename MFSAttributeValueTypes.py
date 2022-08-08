import DataScrape as DS

scrapedFileName = 'MF_DATA_SCRAPED'
MFSData = DS.getData(scrapedFileName)

valTypes = set()
valListLengths = set()
valDictKeys = set()
valStrings = set()
for quote, data in MFSData['MarketWatchHoldingsData'].items():
    if not 'AssetAllocation' in data: continue
    data = data['AssetAllocation']
    attrName = 'Bonds'
    if attrName in data:
        valType = type(data[attrName])
        valTypes.add(valType)
        if valType == list:
            valListLengths.add(len(data[attrName]))
        elif valType == str:
            valStrings.add(data[attrName])
        elif valType == dict:
            for attr, data in data[attrName].items():
                valDictKeys.add(attr)
print('value types       : %s' % valTypes)
print('value list lengths: %s' % valListLengths)
print('value dict keys   : %s' % valDictKeys)
print('value str values  : %s' % valStrings)
