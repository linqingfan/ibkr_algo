import mysql.connector

# Connect to the database
class mysql_connection():

    def __init__(self) -> None:
        self.connection = None

    def connect(self):
        try:
            # creating object of mysql.connector.connect pass required parameters
            self.connection = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="root",
                password="root",
                database="ohlc_data"
            )
            if self.connection.is_connected(): # checking if connection is successfull or not
                print("Connected to MySQL database") # printing message to console
                # Perform operations here
        except mysql.connector.Error as e:
            print("Error connecting to MySQL:", e)
        
    def basic_checks(self):
        try:
            if self.connection.is_connected(): # checking if connection is maintian or not
                    print("Connected to MySQL database") # print message to console

                    cursor = self.connection.cursor() # creating object of cursor - to perform any operation on database table

                    # Check if the Bars table exists
                    cursor.execute("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'ohlc_data'
                    AND table_name = 'Bars'
                    """)

                    table_exists = cursor.fetchone()[0] # fetching query reuslt

                    if table_exists: # checking if table exists or not
                        print("Table 'Bars' already exists.") # print message to console if table exists
                    else:
                        # table not exists create table
                        print("Table 'Bars' does not exist. Creating table...")
                        # Create the Bars table
                        cursor.execute("""
                        CREATE TABLE Bars (
                            symbol VARCHAR(10) NOT NULL,
                            dt DATETIME NOT NULL,
                            open DECIMAL(10, 2),
                            high DECIMAL(10, 2),
                            low DECIMAL(10, 2),
                            close DECIMAL(10, 2),
                            volume INT,
                            PRIMARY KEY (symbol, dt)
                        )
                        """)
                        print("Table 'Bars' created successfully.")
        except Exception as ex:
            print(ex)


    # def insert_data_bars(self,data_to_insert):
    #     # Connect to the database
    #     try:
    #         if self.connection.is_connected(): # checking if connection is maintain or not
    #             print("Connected to MySQL database")
    #             cursor = self.connection.cursor()
    #             # SQL query to insert data into Bars table
    #             insert_query = """
    #             INSERT INTO Bars (symbol, dt, open, high, low, close, volume)
    #             VALUES (%s, %s, %s, %s, %s, %s, %s)
    #             """
    #             # Execute the query with multiple rows
    #             cursor.executemany(insert_query, data_to_insert)
    #             # Commit the transaction
    #             self.connection.commit()
    #             print("Data inserted successfully.")
    #     except mysql.connector.Error as e:
    #         print("Error connecting to MySQL:", e)

    def insert_data_bars(self,symbol,data_to_insert):
        # Connect to the database
        try:
            if self.connection.is_connected(): # checking if connection is maintain or not
                print("Connected to MySQL database")
                cursor = self.connection.cursor()
                cursor.callproc('delete_bar_data', (symbol,))
                self.connection.commit()
                for data in data_to_insert:
                    cursor.callproc('insert_bar_data', data)
                # Commit the transaction
                self.connection.commit()
                print("Data inserted successfully.")
        except mysql.connector.Error as e:
            print("Error connecting to MySQL:", e)


# obj = mysql_connection()
# obj.connect()
# obj.basic_checks()