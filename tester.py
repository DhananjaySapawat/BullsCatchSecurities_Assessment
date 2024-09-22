import pandas as pd
import os
import re 
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, time
import matplotlib.pyplot as plt

# Database connection parameters
db_user = 'postgres'
db_password = '12345678'
db_host = 'localhost'
db_port = '5432'       
db_name = 'Nifty'

def connect_database():
    # Establish connection to the PostgreSQL database
    connection = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    return connection

def CreateOptionChainDatabase(conn):

    option_folder = "NiftyRaw2024Options"
    option_path = f"NIFTYRAW/{option_folder}" 

    # Compile the regex pattern once
    pattern = re.compile(r"([A-Z]+)(\d{6})(\d+)([A-Z]{2})\.csv")

    # Check if the table exists
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'optiontickdata'
        );
    """))
  
    table_exists = result.scalar()  # Fetch the result as a boolean
        
    if table_exists:
        print("Table 'optiontickdata' already exists. Exiting...")
        return  

    # Create the table if it doesn't exist
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS optiontickdata (
            expiry_date varchar(255),
            strike_price INT,
            option_type varchar(255),
            date varchar(255),
            time TIME,
            tick_price DECIMAL(10, 2),
            volume INT,
            open_interest INT
        );
    """))

    # Proceed if the table doesn't exist
    for file in os.listdir(option_path):
        file_path = f"NIFTYRAW/{option_folder}/{file}"
        result = re.findall(pattern, file_path)
        _, expiry_date, strike_price, option_type = result[0] 
        option_label = "Call" if option_type == "CE" else "Put"

        df =  pd.read_csv(file_path)
        df.insert(0, 'expiry_date', int(expiry_date))
        df.insert(1, 'strike_price', int(strike_price))
        df.insert(2, 'option_type', option_label)

        df.to_sql('optiontickdata', conn, if_exists='append', index=False, chunksize=5000)

def CreateNiftyTickDatabase(conn):

    fut_folder = "Nifty_Fut"
    fut_path = f"NIFTYRAW/{fut_folder}" 

    # Check if the table exists
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'niftytickdata'
        );
    """))
  
    table_exists = result.scalar()  # Fetch the result as a boolean
        
    if table_exists:
        print("Table 'niftytickdata' already exists. Exiting...")
        return  

    # Create the table if it doesn't exist
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS niftytickdata (
            date varchar(255),
            open DECIMAL(10, 2),
            close DECIMAL(10, 2),
            volume INT
        );
    """))

    # Proceed if the table doesn't exist
    for file in os.listdir(fut_path):
        file_path = f"NIFTYRAW/{fut_folder}/{file}"
        
        df = pd.read_csv(file_path, header = None)

        # Rename columns for clarity
        df.columns = ['date', 'time', 'tick_price', 'volume', 'open_interest']

        # Group by date and calculate Open, Close, and Volume
        nifty_df = df.groupby('date').agg(
            open=('tick_price', 'first'),
            close=('tick_price', 'last'),
            volume=('volume', 'sum')
        ).reset_index()

        nifty_df.to_sql('niftytickdata', conn, if_exists='append', index=False, chunksize=5000)
        
if __name__ == '__main__':


    # Create the engine
    connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    conn = engine.connect() 

    # task 6 
    specific_option = "NIFTY 15000 CE"
    strike_price = 15000
    option_type = "Call"
    specific_data_query = '''
        SELECT date, AVG(tick_price) AS avg_tick_price
        FROM OPTIOntickdata
        WHERE strike_price = :strike_price AND option_type = :option_type
        GROUP BY date
        ORDER BY date;
    '''
    result = conn.execute(text(specific_data_query), {'strike_price': strike_price, 'option_type': option_type})
    result = result.fetchall()

    x_axis = []
    y_axis = []

    for my_date, my_value in result:
        timestamp  = datetime.strptime(my_date, "%Y-%m-%d").date()
        x_axis.append(timestamp)
        y_axis.append(my_value)

    print(x_axis)
    print(y_axis)
    print(min(x_axis), max(x_axis))
    print(min(y_axis), max(y_axis))

    plt.plot(x_axis, y_axis, marker='o')

    plt.xlabel('X values (Timestamp)')
    plt.ylabel('Y values')

    plt.title(f'Time-Series Plot of LTP for {specific_option}')
    plt.xlabel('Time')
    plt.ylabel('Last Traded Price (LTP)')
    
    plt.grid()
    plt.show()

    # Commit changes and close the connection
    conn.commit()
    conn.close()
