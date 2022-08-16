import DataScrape as DS
import logging
import ExchangeInfo as EI

attributeDocs = {
    'CISOs': {
        '__doc__': 'Country ISO code list'
    },
    'MICs': {
        '__doc__': 'Exchange Market ISO code list',
        'MIC': {
            'Name': {
                '__doc__': 'Descriptive Full Exchange Market Name'
            },
            'Country': {
                '__doc__': "Country Exchange Market is based at in the form of 'XX - Country Name'. XX is the Country ISO code. Used to extract CISO of MIC"
            }
        }
    },
    'MICToCISO': {
        '__doc__': 'Exchange Market ISO code to Country ISO codes Link list '
    },
    'CISOToMIC': {
        '__doc__': 'Country ISO code to Exchange Market ISO codes Link list '
    },
    'MarketWatchQuotes': {
        '__doc__': 'Quotes and base data scraped from MarketWatch Mutual Fund Lists',
        'Quote': {
            'Name': {
                '__doc__': 'Descriptive Full Quote Name. Used for Strategy types in Data Creation'
            },
            'Country': {
                '__doc__': 'Used for Data Creation'
            }
        }
    },
    'MorningStarTypes': {
        '__doc__': 'Types given by MorningStar site redirection, needed to get MorningStar Quote Data',
        'Quote': {
            'Type': {
                '__doc__': 'Found these types so far: FUND, STOCK, CEF, ETF, None '
            }
        }
    },
    'MorningStarTypes': {
        '__doc__': 'Types given by MorningStar site redirection, needed to get MorningStar Quote Data',
        'Quote': {
            'Type': {
                '__doc__': 'Found these types so far: FUND, STOCK, CEF, ETF, None '
            }
        }
    },
    'MarketWatchQuoteData': {
        '__doc__': 'Scraped Quote Data from MarketWatch',
        'Quote': {
            'Type': {
                '__doc__': 'Types given by MarketWatch site redirection. Found these types so far: stock, index, fund'
            },
            'MinInvestment': {
                '__doc__': "Minimum Initial Investment Standard (taxable): [amount, currency] or None . Used for Data Creation"
            },
            'MinInvestmentIRA': {
                '__doc__': "Minimum Initial Investment for IRA: [amount, currency] or None . Non taxable"
            },
            'Yield': {
                '__doc__': "Amount of Yield in %: [amount, unit] or None . Used for Data Creation"
            },
            'NetExpenseRatio': {
                '__doc__': "Net Expense Ratio in %: [amount, unit] or None . Used for Data Creation"
            },
            'TotalExpenseRatio': {
                '__doc__': "Total Expense Ratio in %: [amount, unit] or None"
            }
        }
    },
    'MorningStarQuoteData': {
        '__doc__': 'Scraped Quote Data from MorningStar',
        'Quote': {
            'InvestmentStyle': {
                '__doc__': 'Stock Investment Style = (Small, Mid, Large) (Value, Blend, Growth). Used for Data Creation'
            },
            'CreditQuality': {
                '__doc__': "Bond Investment Style: Credit Quality: 'Low, Medium, High / Limited, Moderate, Extensive' . Value used before the / . Used for Data Creation"
            },
            'InterestRateSensitivity': {
                '__doc__': "Bond Investment Style: Investment Rate Sensitivity: 'Low, Medium, High / Limited, Moderate, Extensive' . Value used after the / . Used for Data Creation"
            },
            'Rating': {
                '__doc__': 'MorningStar Rating. Used for Data Creation'
            },
            'AdjExpenseRatio': {
                '__doc__': "Adjusted Expense Ratio in unit %: float, [amount, unit], '—'"
            },
            'ExpenseRatio': {
                '__doc__': "Expense Ratio in unit %: float, [amount, unit], '—'. Used for Data Creation"
            }
        }
    },
    'MarketWatchHoldingsData': {
        '__doc__': 'Scraped Holdings Data from MarketWatch',
        'Quote': {
            'AssetAllocation': {
                '__doc__': 'Asset Allocation',
                'Stocks': {
                    '__doc__': 'Stocks Allocation in unit %: [amount, unit]. Used for Data Creation'
                },
                'Bonds': {
                    '__doc__': 'Bonds Allocation in unit %: [amount, unit]. Used for Data Creation'
                },
                'Convertible': {
                    '__doc__': 'Convertible Allocation in unit %: [amount, unit]. Used for Data Creation'
                },
                'Other': {
                    '__doc__': 'Other Allocation in unit %: [amount, unit]. Used for Data Creation'
                },
                'Cash': {
                    '__doc__': 'Cash Allocation in unit %: [amount, unit]. Used for Data Creation'
                }
            }
        }
    },
    'YahooFinanceHoldingsData': {
        '__doc__': 'Scraped Holdings Data from YahooFinance',
        'Quote': {
            'AssetAllocation': {
                '__doc__': 'Asset Allocation',
                'Stocks': {
                    '__doc__': 'Stocks Allocation in unit %: [amount, unit], None'
                },
                'Bonds': {
                    '__doc__': 'Bonds Allocation in unit %: [amount, unit], None'
                },
                'Convertable': {
                    '__doc__': 'Convertable Allocation in unit %: [amount, unit], None'
                },
                'Preferred': {
                    '__doc__': 'Preferred Allocation in unit %: [amount, unit], None'
                },
                'Others': {
                    '__doc__': 'Others Allocation in unit %: [amount, unit], None'
                },
                'Cash': {
                    '__doc__': 'Cash Allocation in unit %: [amount, unit], None'
                }
            }
        }
    }
}

def dictRecurse(data, attributes):
    if type(data) == dict:
        for attr, subAttributes in data.items():
            if attr.count(':') == 1: attr = 'Quote'
            
            if not attr in  attributes:
                if attr == 'Quote':
                    attributes[attr] = {'__doc__': "List of Quotes in form 'Symbol:Market Exchange ISO code'"}
                elif attr == 'Holdings':
                    attributes[attr] = {'__doc__': 'Dict with Holdings percentages: {SYMBOL: [amount, unit]}: unit is %'}
                elif attr == 'ScrapeTag':
                    attributes[attr] = {'__doc__': 'Scraping Time Stamp'}
                else:
                    attributes[attr] = {'__doc__': ''}
            
            if attr == 'CISOs':
                attributes[attr]['CISO'] = {'__doc__': 'Country name'}
            elif attr == 'MICToCISO':
                attributes[attr]['MIC'] = {'__doc__': 'List of Country ISO codes where MIC is based at. Should be all just one'}
            elif attr == 'CISOToMIC':
                attributes[attr]['CISO'] = {'__doc__': 'List of Exchange Market ISO code based in this Country ISO code'}
            elif attr == 'MICs':
                attributes[attr]['MIC'] = {'__doc__': 'Exchange Market ISO code'}
                for MIC, micAttributes in subAttributes.items():
                    dictRecurse(micAttributes, attributes[attr]['MIC'])
            elif attr == 'Holdings':
                continue
            else:
                dictRecurse(subAttributes, attributes[attr])

def writeDocs(attributes, attributeDocs):
    for attr, subAttributesDocs in attributeDocs.items():
        if attr in attributes:
            if attr == '__doc__':
                attributes['__doc__'] = subAttributesDocs
            else:
                writeDocs(attributes[attr], subAttributesDocs)

def logAttributes(attributes, level):
    for attr, subAttributes in attributes.items():
        if attr != '__doc__':
            logging.info('%s%s: %s' % ('    ' * level, attr, subAttributes['__doc__']))
            logAttributes(subAttributes, level+1)

if __name__ == "__main__":
    DS.setupLogging('MFSAttributes.log', timed=False, new=True)

    scrapedFileName = 'MF_DATA_SCRAPED'
    # scrapedFileName = 'BASE_DATA_SCRAPED'
    MFSData = DS.getData(scrapedFileName)

    # attributes check
    attributes = {}
    dictRecurse(MFSData, attributes)
    writeDocs(attributes, attributeDocs)
    logAttributes(attributes, 0)
