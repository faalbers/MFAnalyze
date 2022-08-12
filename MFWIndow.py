from PyQt6.QtWidgets import QMainWindow, QVBoxLayout, QCheckBox, QComboBox
from PyQt6 import uic
import DataScrape as DS

class MFWindow(QMainWindow):
    def __init__(self):
        super(MFWindow, self).__init__()

        # load ui file
        uic.loadUi('MFAnalyze.ui', self)

        dataFileName = 'MF_DATA'
        self.MFData = DS.getData(dataFileName)

        self.MFData['CountryToExchanges'] = {}
        for quote, data in self.MFData['Quotes'].items():
            if data['Exchange'] == None: continue
            if data['Country'] == None: continue
            if not data['Country'] in self.MFData['CountryToExchanges']:
                self.MFData['CountryToExchanges'][data['Country']] = set()
            self.MFData['CountryToExchanges'][data['Country']].add(data['Exchange'])
        
        self.MFData['ExchangeToCountries'] = {}
        for quote, data in self.MFData['Quotes'].items():
            if data['Exchange'] == None: continue
            if data['Country'] == None: continue
            if not data['Exchange'] in self.MFData['ExchangeToCountries']:
                self.MFData['ExchangeToCountries'][data['Exchange']] = set()
            self.MFData['ExchangeToCountries'][data['Exchange']].add(data['Country'])
        
        self.countryChecks = QVBoxLayout()
        self.exchangeChecks = QVBoxLayout()
        self.allCountries()
        self.allExchanges()

        self.bondsStocksCheck.setStyleSheet('margin-left:200;');
        self.bondsStocksCheck.setChecked(False)
        self.bondsStocksCheck.stateChanged.connect(self.bondsStocksChanged)
        self.bondsStocksChanged()

        self.minSBRatio.setValue(0)
        self.minSBRatio.valueChanged.connect(self.minSBChanged)

        self.maxSBRatio.setValue(100)
        self.maxSBRatio.valueChanged.connect(self.maxSBChanged)

        # self.setStyle()
        # self.capCombo.currentIndexChanged.connect(self.setStyle)
        # self.styleCombo.currentIndexChanged.connect(self.setStyle)
        # self.qualityCombo.currentIndexChanged.connect(self.setStyle)
        # self.sensitivityCombo.currentIndexChanged.connect(self.setStyle)

        self.msRating1.setChecked(True)
        self.msRating2.setChecked(True)
        self.msRating3.setChecked(True)
        self.msRating4.setChecked(True)
        self.msRating5.setChecked(True)
        self.msRatingNA.setChecked(True)

        self.makeData.clicked.connect(self.updateQuotes)

    def buildCountriesCheckList(self):
        for i in reversed(range(self.countryChecks.count())): 
            self.countryChecks.itemAt(i).widget().setParent(None)
        for country in self.countries:
            checkBox = QCheckBox(country)
            checkBox.setChecked(True)
            checkBox.stateChanged.connect(self.countriesChanged)
            self.countryChecks.addWidget(checkBox)
        self.countriesContents.setLayout(self.countryChecks)
        self.selAllCountries.clicked.connect(self.checkAllCountries)
        self.unSelAllCountries.clicked.connect(self.uncheckAllCountries)
        self.showAllCountries.clicked.connect(self.allCountriesClick)
        # self.updateQuotes()

    def checkAllCountries(self):
        for i in range(self.countryChecks.count()):
            self.countryChecks.itemAt(i).widget().setChecked(True)
    
    def uncheckAllCountries(self):
        for i in range(self.countryChecks.count()):
            self.countryChecks.itemAt(i).widget().setChecked(False)
    
    def allCountries(self):
        self.countries = list(self.MFData['CountryToExchanges'].keys())
        self.countries.sort()
        self.buildCountriesCheckList()
    
    def allCountriesClick(self):
        self.allCountries()
        self.allExchanges()

    def countriesChanged(self):
        self.exchanges = set()
        for i in range(self.countryChecks.count()):
            widget = self.countryChecks.itemAt(i).widget()
            if widget.isChecked() == True:
                for exchange in self.MFData['CountryToExchanges'][self.countries[i]]:
                    self.exchanges.add(exchange)
        self.exchanges = list(self.exchanges)
        self.exchanges.sort()
        self.buildExchangesCheckList()

    def buildExchangesCheckList(self):
        for i in reversed(range(self.exchangeChecks.count())): 
            self.exchangeChecks.itemAt(i).widget().setParent(None)
        for exchange in self.exchanges:
            checkBox = QCheckBox(exchange)
            checkBox.setChecked(True)
            checkBox.stateChanged.connect(self.exchangesChanged)
            self.exchangeChecks.addWidget(checkBox)
        self.exchangeContents.setLayout(self.exchangeChecks)
        self.selAllExchanges.clicked.connect(self.checkAllExchanges)
        self.unSelAllExchanges.clicked.connect(self.uncheckAllExchanges)
        self.showAllExchanges.clicked.connect(self.allExchangesClick)
        # self.updateQuotes()

    def checkAllExchanges(self):
        for i in range(self.exchangeChecks.count()):
            self.exchangeChecks.itemAt(i).widget().setChecked(True)
    
    def uncheckAllExchanges(self):
        for i in range(self.exchangeChecks.count()):
            self.exchangeChecks.itemAt(i).widget().setChecked(False)

    def allExchanges(self):
        self.exchanges = list(self.MFData['ExchangeToCountries'].keys())
        self.exchanges.sort()
        self.buildExchangesCheckList()

    def allExchangesClick(self):
        self.allExchanges()
        self.allCountries()

    def exchangesChanged(self):
        self.countries = set()
        for i in range(self.exchangeChecks.count()):
            widget = self.exchangeChecks.itemAt(i).widget()
            if widget.isChecked() == True:
                for country in self.MFData['ExchangeToCountries'][self.exchanges[i]]:
                    self.countries.add(country)
        self.countries = list(self.countries)
        self.countries.sort()
        self.buildCountriesCheckList()

    def bondsStocksChanged(self):
        if self.bondsStocksCheck.isChecked():
            self.minSBRatio.setEnabled(True)
            self.maxSBRatio.setEnabled(True)
        else:
            self.minSBRatio.setEnabled(False)
            self.maxSBRatio.setEnabled(False)

    # def setStyle(self):
    #     self.stockCap = self.capCombo.currentText()
    #     self.stockStyle = self.styleCombo.currentText()
    #     self.bondCreditQ = self.qualityCombo.currentText()
    #     self.bondInterestRateS = self.sensitivityCombo.currentText()
    #     print(self.stockCap)
    #     print(self.stockStyle)
    #     print(self.bondCreditQ)
    #     print(self.bondInterestRateS)

    def minSBChanged(self):
        value = self.minSBRatio.value()
        if value > self.maxSBRatio.value():
            self.maxSBRatio.setValue(value)

    def maxSBChanged(self):
        value = self.maxSBRatio.value()
        if value < self.minSBRatio.value():
            self.minSBRatio.setValue(value)
    
    def updateQuotes(self):
        countryCodes = set()
        for i in range(self.countryChecks.count()):
            widget = self.countryChecks.itemAt(i).widget()
            if widget.isChecked() == True:
                for countryCode, country in self.MFData['CountryCodes'].items():
                    if self.countries[i] == country:
                        countryCodes.add(countryCode)

        exchangeCodes = set()
        for i in range(self.exchangeChecks.count()):
            widget = self.exchangeChecks.itemAt(i).widget()
            if widget.isChecked() == True:
                for exchangeCode, exchange in self.MFData['ExchangeCodes'].items():
                    if self.exchanges[i] == exchange:
                        exchangeCodes.add(exchangeCode)
        
        quoteEnds = set()
        for exchangeCode in exchangeCodes:
            for countryCode in countryCodes:
                quoteEnds.add(':%s:%s' % (exchangeCode, countryCode))
        # print(quoteEnds)

        stockCap = self.capCombo.currentText()
        stockStyle = self.styleCombo.currentText()
        bondCreditQ = self.qualityCombo.currentText()
        bondInterestRateS = self.sensitivityCombo.currentText()

        ratings = set()
        if self.msRating1.isChecked() == True: ratings.add(1)
        if self.msRating2.isChecked() == True: ratings.add(2)
        if self.msRating3.isChecked() == True: ratings.add(3)
        if self.msRating4.isChecked() == True: ratings.add(4)
        if self.msRating5.isChecked() == True: ratings.add(5)
        if self.msRatingNA.isChecked() == True: ratings.add(None)

        # print(ratings)

        minSBRatio = float(self.minSBRatio.value())
        maxSBRatio = float(self.maxSBRatio.value())

        quotes = []
        for quote, data in self.MFData['Quotes'].items():
            quoteEnd = quote.split(':')
            quoteEnd = ':%s:%s' % (quoteEnd[1], quoteEnd[2])
            if not quoteEnd in quoteEnds: continue

            if not data['MorningStarRating'] in ratings: continue

            if self.bondsStocksCheck.isChecked():
                adata = data['AssetAllocation']
                if adata == None: continue
                if adata['Stocks'] < 0.0 or adata['Bonds'] < 0.0: continue
                total = adata['Stocks'] + adata['Bonds']
                if total == 0.0: continue
                stocks = (adata['Stocks']/total)*100.0
                # bonds = (adata['Bonds']/total)*100.0
                if not (stocks >= minSBRatio and stocks <= maxSBRatio): continue

            if self.minYieldCheck.isChecked():
                if data['Yield'] == None: continue
                elif data['Yield'] < self.minYield.value(): continue

            if self.maxExpenseCheck.isChecked():
                if data['ExpenseRatio'] == None: continue
                elif data['ExpenseRatio'] > self.maxExpense.value(): continue

            if self.maxInvestCheck.isChecked():
                if data['MinInvestment'] == None: continue
                elif data['MinInvestment'] > self.maxInvest.value(): continue

            if stockCap != 'N/A':
                if data['StockStyle'] == None:
                    continue
                elif data['StockStyle']['Cap'] != stockCap:
                    continue

            if stockStyle != 'N/A':
                if data['StockStyle'] == None:
                    continue
                elif data['StockStyle']['Style'] != stockStyle:
                    continue

            if bondCreditQ != 'N/A':
                if data['BondStyle'] == None:
                    continue
                elif data['BondStyle']['CreditQuality'] != bondCreditQ:
                    continue

            if bondInterestRateS != 'N/A':
                if data['BondStyle'] == None:
                    continue
                elif data['BondStyle']['InterestRateSensitivity'] != bondInterestRateS:
                    continue
            
            quotes.append(quote)

        self.quoteCount.display(len(quotes))

        if self.doSaveCSV.isChecked(): self.saveCSV(quotes)

    def saveCSV(self, quotes):
        CSVFileName = 'MF_ANALYZE_DATA.csv'
        out = ''
        out += 'Symbol, Name, Country,'
        out += 'MorningStarRating, ExpenseRatio %, MinInvestment, ETrade, Yield %,'
        out += 'Stocks %, Cap, Style,'
        out += 'Bonds %, CreditQuality, InterestRateSensitivity'
        out += '\n'

        for quote in quotes:
            data = self.MFData['Quotes'][quote]
            quoteSplit = quote.split(':')
            symbol = quoteSplit[0]
            exchange = quoteSplit[1]
            countryCode = quoteSplit[2]
            name = data['Name'].replace(',','')
            
            msrating = 'N/A'
            if data['MorningStarRating'] != None: msrating = data['MorningStarRating']

            Yield = 'N/A'
            if data['Yield'] != None: Yield = data['Yield']

            expense = 'N/A'
            if data['ExpenseRatio'] != None: expense = data['ExpenseRatio']

            mininvest = 'N/A'
            if data['MinInvestment'] != None: mininvest = data['MinInvestment']

            stocks = 'N/A'
            cap = 'N/A'
            style = 'N/A'
            bonds = 'N/A'
            cquality = 'N/A'
            irsensitivity = 'N/A'

            if data['AssetAllocation'] != None:
                stocks = data['AssetAllocation']['Stocks']
                bonds = data['AssetAllocation']['Bonds']

            if data['StockStyle'] != None:
                cap = data['StockStyle']['Cap']
                style = data['StockStyle']['Style']
            
            if data['BondStyle'] != None:
                cquality = data['BondStyle']['CreditQuality']
                irsensitivity = data['BondStyle']['InterestRateSensitivity']
            
            # out filters
            # if countryCode != 'US': continue
            # if msrating == 'N/A': continue

            out += '%s,%s,%s,' % (symbol, name, data['Country'])
            out += '%s,%s,%s,%s,%s,' % (msrating, expense, mininvest, data['ETradeAvailable'], Yield)
            out += '%s,%s,%s,' % (stocks, cap, style)
            out += '%s,%s,%s' % (bonds, cquality, irsensitivity)
            out += '\n'

        with open(CSVFileName, 'w') as f:
            f.write(out)
