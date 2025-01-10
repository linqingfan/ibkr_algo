from ibapi import wrapper
from ibapi.client import EClient, Contract
from ibapi.utils import iswrapper
import pandas as pd
# from Tickers import Ticker
import GlobalVariables
import os
import datetime as datetime1
from datetime import datetime, timedelta
from dbm import mysql_connection
from ibapi.order import Order
from decimal import Decimal
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class TestWrapper(wrapper.EWrapper):
    def __init__(self):
        wrapper.EWrapper.__init__(self)

class IbkrClient(TestWrapper, TestClient):

    def __init__(self):
        TestWrapper.__init__(self) # IBKR wrapper class inherit
        TestClient.__init__(self, wrapper=self) # IBKR client class inherit
        self.nextOrderId = 0
        #self.mysql_connection = mysql_connection()
        #self.mysql_connection.connect()
        #self.mysql_connection.basic_checks()

    @iswrapper
    def connectAck(self):
        """
        Acknowledgement of connection to IBKR API.
        """
        logging.info("Connected to IBKR API.")
        pass

    @iswrapper
    def nextValidId(self, orderId: int):
        """
        Provides the next valid order ID.

        Parameters:
        orderId (int): The next valid order ID from IBKR API.
        """
        super().nextValidId(orderId)
        self.nextOrderId = orderId + 1
        self.loading_tickers() # Load tickers from CSV file
        self.historicalDataOperations_req() #request history data
        #self.tickDataOperations_req() #live data request
        #self.sample_place_order() # shoot trades
        pass

    @iswrapper
    def error(self, reqId, errorCode: int, errorString: str, advancedOrderRejectJson = ""):
        """
        Handles errors received from IBKR API.

        Parameters:
        reqId (int): Request ID.
        errorCode (int): Error code.
        errorString (str): Error message.
        advancedOrderRejectJson (str, optional): Advanced order reject JSON.
        """
        super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
        if advancedOrderRejectJson:
            logging.error(f"Error. Id: {reqId}, Code: {errorCode}, Msg: {errorString}, AdvancedOrderRejectJson: {advancedOrderRejectJson}")
        else:
            logging.error(f"Error. Id: {reqId}, Code: {errorCode}, Msg: {errorString}")
        pass

    @iswrapper
    def winError(self, text: str, lastError: int):
        """
        Handles Windows errors.

        Parameters:
        text (str): Error text.
        lastError (int): Last error code.
        """
        super().winError(text, lastError)
        logging.error(f"WinError: {text}, LastError: {lastError}")

    @iswrapper
    def tickPrice(self, reqId, tickType, price, attrib):
        """
        Handles tick price updates.

        Parameters:
        reqId (int): Request ID.
        tickType (int): Tick type.
        price (float): Tick price.
        attrib (object): Tick attributes.
        """
        logging.info(f"TickPrice. ReqId: {reqId}, TickType: {tickType}, Price: {price}, Attrib: {attrib}")
        pass

    @iswrapper
    def historicalData(self, reqId:int, bar):
        """
        Handles historical data updates.

        Parameters:
        reqId (int): Request ID.
        bar (object): Bar data.
        """
        logging.info(f"HistoricalData. ReqId: {reqId}, BarData: {bar}")
        if reqId in GlobalVariables.tickers_collection:
            objTicker = GlobalVariables.tickers_collection[reqId]
            if not "bars_collection" in objTicker:
                objTicker["bars_collection"] = []
            #dt =  datetime.strptime(bar.date, '%Y%m%d')
            #if dt.date() >= objTicker["history_start_dt"].date() and dt.date() <= objTicker["history_end_dt"].date():
            objTicker["bars_collection"].append(bar)

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """
        Handles end of historical data updates.

        Parameters:
        reqId (int): Request ID.
        start (str): Start time.
        end (str): End time.
        """
        super().historicalDataEnd(reqId, start, end)
        logging.info(f"HistoricalDataEnd. ReqId: {reqId}, from {start} to {end}")
        if reqId in GlobalVariables.tickers_collection:
            objTicker = GlobalVariables.tickers_collection[reqId]
            #self.bars_logging(objTicker["bars_collection"], objTicker["symbol"])
            print(objTicker["symbol"] + " complete")
            if objTicker["symbol"] == "GBPAUD":
                print("")
        self.historicalDataOperations_req()

    @iswrapper
    def orderStatus(self, orderId: int, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        """
        Handles order status updates.

        Parameters:
        orderId (int): Order ID.
        status (str): Order status.
        filled (Decimal): Filled quantity.
        remaining (Decimal): Remaining quantity.
        avgFillPrice (float): Average fill price.
        permId (int): Permanent ID.
        parentId (int): Parent ID.
        lastFillPrice (float): Last fill price.
        clientId (int): Client ID.
        whyHeld (str): Reason held.
        mktCapPrice (float): Market cap price.
        """
        logging.info(f"OrderStatus. OrderId: {orderId}, Status: {status}")

    # function to set values to IBKR API contract object
    def set_ib_contract(self,ticker):
        ib_contract = Contract()
        ib_contract.conId = 0
        ib_contract.secType = ticker["secType"]
        if ib_contract.secType == "CASH":
            ib_contract.symbol = ticker["symbol"][0:3]
            ib_contract.currency = ticker["symbol"][3:6]
        else:   
            ib_contract.symbol = ticker["symbol"]
            ib_contract.currency = ticker["currency"]
        ib_contract.exchange = ticker["exchange"]
        if ib_contract.secType == "FUT":
            ib_contract.lastTradeDateOrContractMonth = ticker["expiry"]
        elif ib_contract.secType == "OPT":
            ib_contract.lastTradeDateOrContractMonth = ticker["expiry"]
            ib_contract.strike = ticker["strike"]
            ib_contract.right = ticker["right"]
        return ib_contract

    def loading_tickers(self):
        try:
            file_to_read= "Tickers.csv" # file name to read for tickers
            if not os.path.exists(file_to_read): # checking if file exists or not
                print(f"File not found {file_to_read}") # if not exists print message
                return # return from function
            df = pd.read_csv(file_to_read) # reading .csv file using pandas function read_csv - it will read data as dataframe
            for index,row in df.iterrows(): # access data from dataframe using iterrows function
                if row['status'] == 'I':
                    continue
                ticker = row.to_dict().copy()
                #ticker["history_start_dt"] = datetime.strptime(ticker["history_start_dt"], '%m/%d/%Y')
                ticker["history_end_dt"] = "" #current datetime
                ticker["ib_contract"] = self.set_ib_contract(ticker) # function to set ib contract
                ticker["historydata_req_done"] = False
                if not ticker["Id"] in GlobalVariables.tickers_collection: # checking if id exists into collection or not
                    GlobalVariables.tickers_collection[ticker["Id"]] =  ticker # if id not exists add key,value into collection
        except Exception as ex:
            print(ex)

    def tickDataOperations_req(self):
        """
        Requests tick data for all tickers in the collection.
        """
        try:
            for tickerId, tickers in GlobalVariables.tickers_collection.items():
                self.reqMktData(tickerId, tickers["ib_contract"], "236", False, False, [])
        except Exception as ex:
            logging.exception("Error requesting tick data.")

    def historicalDataOperations_req(self):
        """
        Requests historical data for all tickers in the collection.
        """
        try:

            for tickerId, tickers in GlobalVariables.tickers_collection.items():
                if tickers["historydata_req_done"] == False:
                    tickers["historydata_req_done"] = True
                    #days = (tickers["history_end_dt"] - tickers["history_start_dt"]).days
                    #queryTime = (tickers["history_end_dt"]).strftime("%Y%m%d-%H:%M:%S")
                    curTime = datetime.now(datetime1.timezone.utc).strftime("%Y%m%d-%H:%M:%S")
                    if tickers["secType"]=="CASH":
                        whatToShow="MIDPOINT"
                    elif tickers["secType"] == "STK":
                        whatToShow="TRADES"
                    elif tickers["secType"] == "FUT":
                        whatToShow="SCHEDULE"
                    self.reqHistoricalData(tickerId, tickers["ib_contract"], curTime,tickers["duration"], tickers["barsize"], whatToShow, 1, 1, False, [])
                    break
        except Exception as ex:
            logging.exception("Error requesting historical data.")

    def bars_logging(self, data_collection, symbol):
        """
        Inserts bar data into the database.

        Parameters:
        data_collection (list): List of bar data.
        symbol (str): Ticker symbol.
        """
        try:
            iscsv = True
            isbd = False
            if iscsv:
                temp_data = []
                for bar in data_collection:
                    temp = {"symbol":symbol,"date":bar.date,"open":bar.open,"high":bar.high,"low":bar.low,"close":bar.close,"volume":int(bar.volume)}
                    temp_data.append(temp)
                df = pd.DataFrame(temp_data)
                df.to_csv(f"{symbol}.csv")
            if isbd:
                ## History data to database code commented
                temp_data = []
                for bar in data_collection:
                    # dt =  datetime.strptime(bar.date, '%Y%m%d')
                    temp = [symbol,bar.date,bar.open,bar.high,bar.low,bar.close,bar.volume]
                    temp_data.append(temp)
                self.mysql_connection.insert_data_bars(symbol,temp_data)

        except Exception as ex:
            logging.exception("Error inserting bars to database.")

    def LimitOrder(self, action: str, quantity: Decimal, limitPrice: float) -> Order:
        """
        Creates a limit order.

        Parameters:
        action (str): Order action (BUY/SELL).
        quantity (Decimal): Order quantity.
        limitPrice (float): Limit price.

        Returns:
        Order: The limit order object.
        """
        order = Order()
        order.action = action
        order.orderType = "LMT"
        order.totalQuantity = quantity
        order.lmtPrice = limitPrice
        return order

    def MarketOrder(self, action: str, quantity: Decimal) -> Order:
        """
        Creates a market order.

        Parameters:
        action (str): Order action (BUY/SELL).
        quantity (Decimal): Order quantity.

        Returns:
        Order: The market order object.
        """
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = quantity
        return order

    def sample_place_order(self):
        """
        Places a sample market order for each ticker in the collection.
        """
        try:
            for tickerId, tickers in GlobalVariables.tickers_collection.items():
                mkt = self.MarketOrder(tickers["action"],tickers["qty"])
                self.placeOrder(self.nextOrderId, tickers["ib_contract"], mkt)
                logging.info(f"Placed order with orderId: {self.nextOrderId}")
                self.nextOrderId += 1
        except Exception as ex:
            logging.exception("Error placing sample order.")

def main():
    """
    Main function to initialize and run the TestApp.
    """
    try:
        app = IbkrClient()#create an object for a class called as TestApp()
        app.connect("127.0.0.1", 7497, clientId=1)
        app.run()
    except Exception as ex:
        logging.exception("Error in main function.")

if __name__ == "__main__":
    main() #function or a method
