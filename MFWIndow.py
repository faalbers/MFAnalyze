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
        self.MFData['ExchangeToCountries'] = {}
        self.MFData['FundTypes'] = set()
        for quote, data in self.MFData['Quotes'].items():
            if not data['Exchange']['Country'] in self.MFData['CountryToExchanges']:
                self.MFData['CountryToExchanges'][data['Exchange']['Country']] = set()
            self.MFData['CountryToExchanges'][data['Exchange']['Country']].add(data['Exchange']['Name'])
            
            if not data['Exchange']['Name'] in self.MFData['ExchangeToCountries']:
                self.MFData['ExchangeToCountries'][data['Exchange']['Name']] = set()
            self.MFData['ExchangeToCountries'][data['Exchange']['Name']].add(data['Exchange']['Country'])

            self.MFData['FundTypes'].add(data['Fund']['Type'])
        
        self.countryChecks = QVBoxLayout()
        self.exchangeChecks = QVBoxLayout()
        self.fundTypeChecks = QVBoxLayout()
        self.allCountries()
        self.allExchanges()
        self.allFundTypes()

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

    def buildFundTypesCheckList(self):
        for i in reversed(range(self.fundTypeChecks.count())): 
            self.fundTypeChecks.itemAt(i).widget().setParent(None)
        for fundType in self.fundTypes:
            checkBox = QCheckBox(fundType)
            checkBox.setChecked(True)
            # checkBox.stateChanged.connect(self.countriesChanged)
            self.fundTypeChecks.addWidget(checkBox)
        self.fundTypeContents.setLayout(self.fundTypeChecks)
        # self.selAllCountries.clicked.connect(self.checkAllCountries)
        # self.unSelAllCountries.clicked.connect(self.uncheckAllCountries)
        # self.showAllCountries.clicked.connect(self.allCountriesClick)
        # # self.updateQuotes()

    def allFundTypes(self):
        self.fundTypes = list(self.MFData['FundTypes'])
        self.fundTypes.sort()
        self.buildFundTypesCheckList()

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
        exchangeNames = set()
        for i in range(self.exchangeChecks.count()):
            widget = self.exchangeChecks.itemAt(i).widget()
            if widget.isChecked() == True:
                exchangeNames.add(self.exchanges[i])

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

        fundTypes = set()
        for i in range(self.fundTypeChecks.count()):
            widget = self.fundTypeChecks.itemAt(i).widget()
            if widget.isChecked() == True:
                fundTypes.add(self.fundTypes[i])

        quotes = []
        for quote, data in self.MFData['Quotes'].items():
            if not data['Exchange']['Name'] in exchangeNames: continue

            if not data['Data']['MorningStarRating'] in ratings: continue
            
            if not data['Fund']['Type'] in fundTypes: continue

            if self.bondsStocksCheck.isChecked():
                sbRatio = data['Data']['AssetAllocation']['StocksBondsRatio']
                if sbRatio == None: continue
                if not (sbRatio >= minSBRatio and sbRatio <= maxSBRatio): continue

            if self.minYieldCheck.isChecked():
                if data['Data']['Yield'] == None: continue
                if data['Data']['Yield'] < self.minYield.value(): continue

            if self.maxExpenseCheck.isChecked():
                edata = data['Data']['Expense']
                if edata['NetExpenseRatio'] == None and edata['AdjExpenseRatio'] == None: continue
                expenseRatio = 0.0
                if edata['NetExpenseRatio'] != None and edata['NetExpenseRatio'] > expenseRatio:
                    expenseRatio = edata['NetExpenseRatio']
                if edata['AdjExpenseRatio'] != None and edata['AdjExpenseRatio'] > expenseRatio:
                    expenseRatio = edata['AdjExpenseRatio']
                if expenseRatio > self.maxExpense.value(): continue
            
            if self.maxInvestCheck.isChecked():
                if data['Data']['MinInvestment'] != None and data['Data']['MinInvestment'] > self.maxInvest.value(): continue

            if stockCap != 'N/A':
                if data['Data']['Stocks']['Cap'] != stockCap: continue

            if stockStyle != 'N/A':
                if data['Data']['Stocks']['Style'] != stockStyle: continue

            if bondCreditQ != 'N/A':
                if data['Data']['Bonds']['CreditQuality'] != bondCreditQ: continue

            if bondInterestRateS != 'N/A':
                if data['Data']['Bonds']['InterestRateSensitivity'] != bondInterestRateS: continue
            
            quotes.append(quote)

        self.quoteCount.display(len(quotes))

        if self.doSaveCSV.isChecked(): self.saveCSV(quotes)

    def saveCSV(self, quotes):
        CSVFileName = 'MF_ANALYZE_DATA.csv'
        out = ''
        out += 'Symbol, Name, Type, MSRating,'
        out += 'ExpenseRatio %, Yield %,'
        out += 'Stocks %, Cap, Style,'
        out += 'Bonds %, CreditQuality, InterestRateSensitivity,'
        out += 'MinInvestment, ETrade,'
        out += 'Exchange'
        out += '\n'

        for quote in quotes:
            fund = self.MFData['Quotes'][quote]['Fund']
            exchange = self.MFData['Quotes'][quote]['Exchange']
            data = self.MFData['Quotes'][quote]['Data']

            symbol = '"%s"' % fund['Symbol']
            name = '"%s"' % fund['Name']
            ftype = '"%s"' % fund['Type']
            exchangeName = '"%s"' % exchange['Name']

            msrating = 'N/A'
            if data['MorningStarRating'] != None: msrating = data['MorningStarRating']
            
            expenseRatio = 'N/A'
            edata = data['Expense']
            if not (edata['NetExpenseRatio'] == None and edata['AdjExpenseRatio'] == None):
                expenseRatio = 0.0
                if edata['NetExpenseRatio'] != None and edata['NetExpenseRatio'] > expenseRatio:
                    expenseRatio = edata['NetExpenseRatio']
                if edata['AdjExpenseRatio'] != None and edata['AdjExpenseRatio'] > expenseRatio:
                    expenseRatio = edata['AdjExpenseRatio']

            Yield = 'N/A'
            if data['Yield'] != None: Yield = data['Yield']

            mininvest = 'N/A'
            if data['MinInvestment'] != None: mininvest = data['MinInvestment']

            etradeAvailable = 'N/A'
            if data['ETradeAvailbility'] != None: etradeAvailable = '"%s"' % data['ETradeAvailbility']

            aadata = data['AssetAllocation']
            stocks = 'N/A'
            if aadata['Stocks'] != None: stocks = aadata['Stocks']
            
            bonds = 'N/A'
            if aadata['Bonds'] != None: bonds = aadata['Bonds']

            sdata = data['Stocks']
            cap = 'N/A'
            if sdata['Cap'] != None: cap = sdata['Cap']
            
            style = 'N/A'
            if sdata['Style'] != None: style = sdata['Style']

            bdata = data['Bonds']
            cquality = 'N/A'
            if bdata['CreditQuality'] != None: cquality = bdata['CreditQuality']
            
            irsensitivity = 'N/A'
            if bdata['InterestRateSensitivity'] != None: irsensitivity = bdata['InterestRateSensitivity']

            out += '%s,%s,%s,%s,' % (symbol, name, ftype, msrating)
            out += '%s,%s,' % (expenseRatio, Yield)
            out += '%s,%s,%s,' % (stocks, cap, style)
            out += '%s,%s,%s,' % (bonds, cquality, irsensitivity)
            out += '%s,%s,' % (mininvest, etradeAvailable)
            out += '%s' % (exchangeName)
            out += '\n'

        with open(CSVFileName, 'w') as f:
            f.write(out)
    
    def saveCSVOLD(self, quotes):
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
