import DataScrape as DS
import logging, statistics
from logging.handlers import RotatingFileHandler

MFData = DS.getData('MF_DATA_MW_QD')

# for symbol, data in MFData.items():
#     if 'Beta' in data and len(data['Beta']) == 3:
#         print(symbol)

print(MFData['159910']['Beta'])

# dataFileNames = ['MF_DATA_MW_Q', 'MF_DATA_MS_T', 'MF_DATA_MS_QD', 'MF_DATA_MW_QD', 'MF_DATA_YF_QD']
# # dataFileNames = ['MF_DATA_MW_Q', 'MF_DATA_MS_T']
# for dataFileName in dataFileNames:
#     logger = DS.getLogger(dataFileName+'.log', new=True)
#     MFData = DS.getData(dataFileName)

#     logger.info('')
#     logger.info('--- %s ---' % dataFileName )

#     attributes = {}
#     valueAttribute = 'FeeLevel'
#     values = set()
#     for symbol, data in MFData.items():
#         for attribute in data.keys():
#             if not attribute in attributes:
#                 attributes[attribute] = [1, 0]
#             else:
#                 attributes[attribute][0] += 1
#             attributes[attribute][1] = max(attributes[attribute][1], len(data[attribute]))
#         if valueAttribute in data:
#             for value in data[valueAttribute]:
#                 values.add(value)
    
#     attrList = list(attributes.keys())
#     attrList.sort()

#     for attribute in attrList:
#         data = attributes[attribute]
#         logger.info('%s: %s items: %s' % (attribute, data[0], data[1]))




# logging.info('%s: %s' % (valueAttribute, values))

# Low = []
# BelowAverage = []
# Average = []
# AboveAverage = []
# High = []
# classes = set()
# for attribute, data in MFData.items():
#     if 'FeeLevel' in data:
#         # it is one value or a list
#         expenseRatio = None
#         if type(data['ExpenseRatio'][0]) == list:
#             # all first ones of lists are floats
#             expenseRatio = data['ExpenseRatio'][0][0]
#         else:
#             # some non lists are float = zero or string = '-'
#             # we skip these
#             continue

#         if data['FeeLevel'][0] == 'Low':
#             Low.append(expenseRatio)
#         if data['FeeLevel'][0] == 'Below Average':
#             BelowAverage.append(expenseRatio)
#         if data['FeeLevel'][0] == 'Average':
#             Average.append(expenseRatio)
#         if data['FeeLevel'][0] == 'Above Average':
#             AboveAverage.append(expenseRatio)
#         if data['FeeLevel'][0] == 'High':
#             High.append(expenseRatio)

# logging.info('Low          : %s <-> %s: %s' % (min(Low), max(Low), statistics.median(Low)))
# logging.info('Below Average: %s <-> %s: %s' % (min(BelowAverage), max(BelowAverage), statistics.median(BelowAverage)))
# logging.info('Average      : %s <-> %s: %s' % (min(Average), max(Average), statistics.median(Average)))
# logging.info('Above Average: %s <-> %s: %s' % (min(AboveAverage), max(AboveAverage), statistics.median(AboveAverage)))
# logging.info('High         : %s <-> %s: %s' % (min(High), max(High), statistics.median(High)))
