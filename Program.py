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
import time
from threading import Thread,Semaphore
import sys

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger

import pytz
import multiprocess
cpu_count = multiprocess.cpu_count()
print(f'Number of logical CPU cores: {cpu_count}')



DEFAULT_HOST = '127.0.0.1'
DEFAULT_CLIENT_ID = 1

LIVE_TRADING = False
LIVE_TRADING_PORT = 7496
PAPER_TRADING_PORT = 7497
TRADING_PORT = PAPER_TRADING_PORT
if LIVE_TRADING:
    TRADING_PORT = LIVE_TRADING_PORT

# Configure logging
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class TestWrapper(wrapper.EWrapper):
    def __init__(self):
        wrapper.EWrapper.__init__(self)

class IbkrClient(TestWrapper, TestClient):

    def __init__(self,host=DEFAULT_HOST,port=TRADING_PORT,client_id=DEFAULT_CLIENT_ID):
        TestWrapper.__init__(self) # IBKR wrapper class inherit
        TestClient.__init__(self, wrapper=self) # IBKR client class inherit
        self.account_values = {}
        self.margin_values ={}
        self.pending_orders = {}  # List for pending orders
        self.filled_orders = {}  # List for filled orders
        #self.mysql_connection = mysql_connection()
        #self.mysql_connection.connect()
        #self.mysql_connection.basic_checks()
        self.sem = Semaphore()
        self.connect(host, port, clientId=client_id)
        thread = Thread(target=self.run)
        thread.start()

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
        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextOrderId = orderId

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState):
        self.order_state = orderState
        print(f"Order State - Status: {orderState.status}, "
              f"Init Margin After: {orderState.initMarginAfter}, "
              f"Maint Margin After: {orderState.maintMarginAfter}")
        if orderState.status=="PreSubmitted":
            self.margin_values[orderId]["init_margin"]=orderState.initMarginAfter
            self.margin_values[orderId]["maint_margin"]=orderState.initMarginAfter
            self.margin_values[orderId]["equity_with_loan"]=orderState.equityWithLoanAfter
            self.margin_values[orderId]["complete"]=True
        if orderState.status=="Submitted":
            self.pending_orders[orderId]=""

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
        #t = datetime.datetime.fromtimestamp(int(bar.date))
        t = bar.date.split(' ')
        if len(t)>1:
            t = t[0]+' '+t[1]
            t = datetime.strptime(t, '%Y%m%d %H:%M:%S')
        else:
            t = datetime.strptime(t[0], '%Y%m%d')
        # creation bar dictionary for each bar received
        data = {
            'date': t,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': int(bar.volume)
        }
        logging.info(f"HistoricalData. ReqId: {reqId}, BarData: {bar}")
        if reqId in GlobalVariables.tickers_collection:
            objTicker = GlobalVariables.tickers_collection[reqId]
            if not "bars_collection" in objTicker:
                objTicker["bars_collection"] = []
            else:
                last_dt = objTicker["bars_collection"][-1]['date']
                if t == last_dt:
                    return
            objTicker["bars_collection"].append(data)
            if objTicker["historydata_periodic"] == True:
                objTicker["bars_collection"].pop(0)
                objTicker["historydata_periodic"] = False

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
            objTicker["historydata_complete"]=True
            print(objTicker["symbol"] + " complete")

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
        #logging.info(f"OrderStatus. OrderId: {orderId}, Status: {status}")
        print(f"orderId: {orderId}, status: {status}, filled: {filled}, remaining: {remaining}, avgFillPrice: {avgFillPrice}, permId: {permId}, parentId: {parentId}, lastFillPrice: {lastFillPrice}, clientId: {clientId}, whyHeld: {whyHeld}, mktCapPrice: {mktCapPrice}")
        if status == "Filled" and int(remaining)==0:
            try:
                del self.pending_orders[orderId] #sometime duplicate callbacks
            except:
                pass
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
            #GlobalVariables.tickers_collection["period"] = []
            df = pd.read_csv(file_to_read) # reading .csv file using pandas function read_csv - it will read data as dataframe
            for index,row in df.iterrows(): # access data from dataframe using iterrows function
                if row['status'] == 'I':
                    continue
                ticker = row.to_dict().copy()
                #if not(ticker["barsize"] in GlobalVariables.tickers_collection["period"]):
                #    GlobalVariables.tickers_collection["period"].append(ticker["barsize"])
                #ticker["history_start_dt"] = datetime.strptime(ticker["history_start_dt"], '%m/%d/%Y')
                ticker["history_end_dt"] = "" #current datetime
                ticker["ib_contract"] = self.set_ib_contract(ticker) # function to set ib contract
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
                tickers["historydata_complete"] = False
                tickers["historydata_periodic"] = False
                #days = (tickers["history_end_dt"] - tickers["history_start_dt"]).days
                #queryTime = (tickers["history_end_dt"]).strftime("%Y%m%d-%H:%M:%S")
                curTime = datetime.now(datetime1.timezone.utc).strftime("%Y%m%d-%H:%M:%S")
                if tickers["secType"]=="CASH":
                    whatToShow="MIDPOINT"
                elif tickers["secType"] == "STK":
                    whatToShow="TRADES"
                elif tickers["secType"] == "FUT":
                    whatToShow="SCHEDULE"
                #self.reqHistoricalData(tickerId, tickers["ib_contract"], curTime,tickers["duration"], tickers["barsize"], whatToShow, 1, 1, False, [])
                self.reqHistoricalData(tickerId, tickers["ib_contract"], "",tickers["duration"], tickers["barsize"], whatToShow, 1, 1, False, [])
        except Exception as ex:
            logging.exception("Error requesting historical data.")
    def historicalDataOperations_onebar_req(self,tickerId,tickers):
        """
        Requests historical data for all tickers in the collection.
        """
        try:
            tickers["historydata_complete"] = False
            tickers["historydata_periodic"] = True
            #days = (tickers["history_end_dt"] - tickers["history_start_dt"]).days
            #queryTime = (tickers["history_end_dt"]).strftime("%Y%m%d-%H:%M:%S")
            curTime = datetime.now(datetime1.timezone.utc).strftime("%Y%m%d-%H:%M:%S")
            if tickers["secType"]=="CASH":
                whatToShow="MIDPOINT"
            elif tickers["secType"] == "STK":
                whatToShow="TRADES"
            elif tickers["secType"] == "FUT":
                whatToShow="SCHEDULE"
            bartext=tickers["barsize"].split(' ')
            numberofbar=bartext[0]
            if bartext[1] == "secs" or bartext[1] == "sec":
                duration = numberofbar+" S"
            elif bartext[1] == "mins" or bartext[1] == "min":
                duration = str(int(numberofbar)*60) + " S"
            elif bartext[1] == "hours" or bartext[1] == "hour":
                duration = str(int(numberofbar)*60*60) + " S"
            elif bartext[1] == "day":
                duration = numberofbar+" D" #only 1 day is valid
            elif bartext[1] == "week":
                duration = numberofbar+" W" #only 1 week is valid
            elif bartext[1] == "month":
                duration = numberofbar+" M" #only 1 month is valid
            else:
                print("invalid barsize")
                sys.exit()
            #self.reqHistoricalData(tickerId, tickers["ib_contract"], curTime,duration, tickers["barsize"], whatToShow, 1, 1, False, [])
            self.reqHistoricalData(tickerId, tickers["ib_contract"], "",duration, tickers["barsize"], whatToShow, 1, 1, False, [])
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
def waitForData():
    while(True):
        status = True
        for Id,ticker in GlobalVariables.tickers_collection.items():
            if ticker["historydata_complete"]==False:
                status = False
                break
        if status:
            break
        time.sleep(1)

def waitfor1bar(ticker):
    while not ticker['historydata_complete']:
        time.sleep(0.1)
def process_algo(app,barsize):
    # read newest bar first
    print(f"Job barsize={barsize} is running on {datetime.now(pytz.timezone('GMT'))}")
    for tickerId, ticker in GlobalVariables.tickers_collection.items():
        if ticker['barsize'] == barsize:
            app.historicalDataOperations_onebar_req(tickerId,ticker)
            waitfor1bar(ticker)
            print("Process new data with algo here:")
            # e.g. to test if order is at comfortable margin level
            #Create a market order with WhatIf flag set to True
            order = Order()
            order.action = "BUY"
            order.orderType = "MKT"
            order.totalQuantity = 100
            try:
                if check_margin(app, ticker['ib_contract'],order):
                    print("This order is within comfortable margin to trade")
            except:
                print("Check margin error")
            # Note that the datetime collected is in local exchange time i.e. if is in HK, it will be in HK time
            print(ticker['bars_collection'][0]['date'],ticker['bars_collection'][-1]['date'])
def schedule_cron(app):
    df=pd.DataFrame.from_dict(GlobalVariables.tickers_collection,orient="index")
    df = df.sort_values('barsize').set_index(['barsize','Id'])
    groups=df.index.unique(0) # number of groups with same barsize
    # Create a scheduler
    executors = {
        #'default': ThreadPoolExecutor(20),  # Max 20 threads
        'processpool': ProcessPoolExecutor(5)  # Max 5 processes
    }
    scheduler = BackgroundScheduler(executors=executors,timezone=pytz.timezone('GMT'))
    for i,barsize in enumerate(groups):
        bartext=barsize.split(' ')
        numberofbar=int(bartext[0])
        if bartext[1] == "secs" or bartext[1] == "sec":
            scheduler.add_job(process_algo, CronTrigger(second=f'*/{numberofbar}'), args=[app,barsize], id=f'job_{numberofbar}_seconds')
            #scheduler.add_job(job_function, 'cron', args=[barsize,df], minute='*', second=f'{numberofbar}', id=f'job_{i}')
        elif bartext[1] == "mins" or bartext[1] == "min":
            scheduler.add_job(process_algo, CronTrigger(minute=f'*/{numberofbar}'), args=[app,barsize], id=f'job_{numberofbar}_mins')
        elif bartext[1] == "hours" or bartext[1] == "hour":
            scheduler.add_job(process_algo, CronTrigger(hour=f'*/{numberofbar}'), args=[app,barsize], id=f'job_{numberofbar}_hours')
        elif bartext[1] == "day": #only daily # GMT 12pm is EST 7am and Asia/HK 8pm
            scheduler.add_job(process_algo, CronTrigger(hour=12, minute=0), args=[app,barsize], id='job_daily')
        elif bartext[1] == "week": #only weekly
            scheduler.add_job(process_algo, CronTrigger(day_of_week='sun', hour=0, minute=0), args=[app,barsize], id='job_weekly')
        elif bartext[1] == "month": #only monthly
            scheduler.add_job(process_algo, CronTrigger(day=27, hour=0, minute=0), args=[app,barsize], id='job_monthly')
    scheduler.start()

    try:
        # Keep the script running to allow jobs to execute
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # Shutdown the scheduler when exiting the script
        scheduler.shutdown()
def BracketOrder(parentOrderId:int, action:str, quantity:int,limitPrice:float, takeProfitLimitPrice:float, stopLossPrice:float):
        #This will be our main or "parent" order
        parent = Order()
        parent.orderId = parentOrderId
        parent.action = action
        parent.orderType = "LMT"
        parent.totalQuantity = quantity
        parent.lmtPrice = limitPrice
        #The parent and children orders will need this attribute set to False to prevent accidental executions.
        #The LAST CHILD will have it set to True, 
        parent.transmit = False
        takeProfit = Order()
        takeProfit.orderId = parent.orderId + 1
        takeProfit.action = "SELL" if action == "BUY" else "BUY"
        takeProfit.orderType = "LMT"
        takeProfit.tif = "GTC"
        takeProfit.totalQuantity = quantity
        takeProfit.lmtPrice = takeProfitLimitPrice
        takeProfit.parentId = parentOrderId
        takeProfit.transmit = False
        stopLoss = Order()
        stopLoss.orderId = parent.orderId + 2
        stopLoss.action = "SELL" if action == "BUY" else "BUY"
        stopLoss.orderType = "STP"
        stopLoss.tif = "GTC"
        #Stop trigger price
        stopLoss.auxPrice = stopLossPrice
        stopLoss.totalQuantity = quantity
        stopLoss.parentId = parentOrderId
        #In this case, the low side order will be the last child being sent. Therefore, it needs to set this attribute to True 
        #to activate all its predecessors
        stopLoss.transmit = True
        bracketOrder = [parent, takeProfit, stopLoss]
        return bracketOrder
def waitForOrderStatus(app,orderID):
    while not app.margin_values[orderID]["complete"]:
        time.sleep(0.1)
def check_margin(api: IbkrClient, contract,order):
    #Ensure it is for checking purpose, not real order
    order.whatIf = True  # Enable WhatIf functionality
    #order.whatIf = False  # Enable WhatIf functionality
    # Place the what-if order
    api.sem.acquire()
    #ensure atomic action using semaphore
    api.margin_values[api.nextOrderId]={}
    api.margin_values[api.nextOrderId]['complete']=False
    api.placeOrder(api.nextOrderId, contract, order)
    orderID=api.nextOrderId
    waitForOrderStatus(api,orderID)
    api.nextOrderId += 1
    ratio = float(api.margin_values[orderID]['maint_margin'])/float(api.margin_values[orderID]['equity_with_loan'])
    try:
        del api.margin_values[orderID]
    except KeyError:
        pass
    api.sem.release()

    # 50% margin of safety to avoid margin call
    if ratio <0.5:
        return True
    else:
        return False
def main():
    """
    Main function to initialize and run the TestApp.
    """
    try:
        app = IbkrClient()#create an object for a class called as TestApp()
        app.loading_tickers() # Load tickers from CSV file
        app.historicalDataOperations_req()
        waitForData()
        print("At this point, all bars in tickers have been loaded into GlobalVariables.tickers_collection")
        for Id,ticker in GlobalVariables.tickers_collection.items():
            print("%s have %d bars"%(ticker["symbol"],len(ticker['bars_collection'])))
        # bars for each ticker are stored in GlobalVariables.tickers_collection[Id]['bars_collection']
        # start periodic reading of each time bar
        schedule_cron(app)

    except Exception as ex:
        logging.exception("Error in main function.")

if __name__ == "__main__":
     main() #function or a method
