import os
import re
from datetime import datetime, time, timedelta
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
db_host = os.getenv('db_host')
db_port = os.getenv('db_port')     
db_name = os.getenv('db_name')

def is_valid_date(date_string):
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    return bool(re.match(pattern, date_string))

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

    CreateOptionChainDatabase(conn)
    CreateNiftyTickDatabase(conn)

    # task 1
    print("-" * 25, "Task 1", "-" * 25)
    tables = ["optiontickdata", "niftytickdata"]
    columns = {
        "optiontickdata": ["strike_price", "volume", "open_interest"],
        "niftytickdata": ["open", "close", "volume"]
    }
    for table in tables:
        max_query = f"SELECT MAX({columns[table][0]}), MAX({columns[table][1]}), MAX({columns[table][2]}) FROM {table};"
        min_query = f"SELECT MIN({columns[table][0]}), MIN({columns[table][1]}), MIN({columns[table][2]}) FROM {table};"
        mean_query = f"SELECT AVG({columns[table][0]}), AVG({columns[table][1]}), AVG({columns[table][2]}) FROM {table};"
        median_query = f'''
            SELECT 
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {columns[table][0]}),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {columns[table][1]}),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {columns[table][2]})
            FROM 
                {table};
        '''
        queries = {
            "Max Values": max_query,
            "Min Values": min_query,
            "Mean Values": mean_query,
            "Median Values": median_query
        }

        for query_type, query in queries.items():
            result = conn.execute(text(query))
            values = result.fetchone()
            print(f"{query_type}: {columns[table][0]} = {values[0]}, {columns[table][1]} = {values[1]}, {columns[table][2]} = {values[2]}")


    # task 2 
    print("-" * 25, "Task 2", "-" * 25)
    result = conn.execute(text('SELECT option_type, SUM(volume) FROM optiontickdata GROUP BY option_type;'))
    result = result.fetchall()

    for option_type, total_volume in result:
        print(f"Option Type: {option_type}, Total Volume: {total_volume}")


    # task 3
    print("-" * 25, "Task 3", "-" * 25)
    option_cleaning_query = '''
        DELETE FROM optiontickdata
        WHERE expiry_date IS NULL
            OR strike_price IS NULL
            OR tick_price IS NULL
            OR volume IS NULL
            OR open_interest IS NULL;
    '''
    conn.execute(text(option_cleaning_query))

    nifty_cleaning_query = '''
        DELETE FROM niftytickdata
        WHERE open IS NULL
            OR close IS NULL
            OR volume IS NULL;    
    '''
    conn.execute(text(nifty_cleaning_query))

    #task 4
    print("-" * 25, "Task 4", "-" * 25)
    task4_query = '''
        SELECT expiry_date 
        FROM optiontickdata 
        LIMIT 1;
    '''
    result = conn.execute(text(task4_query))
    result = result.fetchone()
    pattern = r'^\d{4}-\d{2}-\d{2}$'

    if bool(re.match(pattern, result[0])) == False:
        option_format_query = '''
            UPDATE optiontickdata
            SET 
                expiry_date = TO_DATE(expiry_date, 'DDMMYY'),
                date = TO_DATE(date, 'YYYYMMDD');
        '''
        conn.execute(text(option_format_query))

        nifty_format_query = '''
            UPDATE niftytickdata
            SET 
                date = TO_DATE(date, 'YYYYMMDD');

        '''
        conn.execute(text(nifty_format_query))

    # task 5
    print("-" * 25, "Task 5", "-" * 25)
    filter_query = '''
        SELECT * FROM optiontickdata
        WHERE strike_price > 15000 AND volume > 1000
        LIMIT 5;
    '''
    result = conn.execute(text(filter_query))
    result = result.fetchall()

    df = pd.DataFrame(result, columns=['expire_date', 'strike_price', 'option_type', 'date', 'time', 'tick_price', 'volume', 'open_interest'])
    print(df)

    # task 6
    print("-" * 25, "Task 6", "-" * 25)
    specific_option = "NIFTY 15000 CE"
    strike_price = 15000
    option_type = "Call"
    specific_data_query = '''
        SELECT date, time, AVG(tick_price) AS avg_tick_price
        FROM OPTIOntickdata
        WHERE strike_price = :strike_price AND option_type = :option_type
        GROUP BY date, time
        ORDER BY date, time;
    '''
    result = conn.execute(text(specific_data_query), {'strike_price': strike_price, 'option_type': option_type})
    result = result.fetchall()

    x_axis = []
    y_axis = []

    for my_date, my_time, my_value in result:
        timestamp = datetime.strptime(f"{my_date} {my_time}", "%Y-%m-%d %H:%M:%S")
        x_axis.append(timestamp)
        y_axis.append(my_value)

    plt.figure(figsize=(12, 6))
    plt.plot(x_axis, y_axis)

    plt.title(f'Time-Series Plot of LTP for {specific_option}')
    plt.xlabel('Timestamp')
    plt.ylabel('Last Traded Price (LTP)')
    
    plt.grid(True)
    plt.savefig("Graphs/task6.png", dpi = 200)
    plt.close() 

    # task 7 
    print("-" * 25, "Task 7", "-" * 25)
    option_data_query = '''
        SELECT date, time, open_interest
        FROM optiontickdata AS O1
        WHERE O1.expiry_date = (SELECT MAX(expiry_date) FROM optiontickdata) AND option_type = :option_type
        ORDER BY date, time;
    '''

    x_axis = {'Put' : [], 'Call' : []}
    y_axis = {'Put' : [], 'Call' : []}

    for option_type in ['Put', 'Call']:
        result = conn.execute(text(option_data_query), {'option_type': option_type})
        result = result.fetchall()

        for my_date, my_time, open_interest in result:
            timestamp = datetime.strptime(f"{my_date} {my_time}", "%Y-%m-%d %H:%M:%S")
            x_axis[option_type].append(timestamp)
            y_axis[option_type].append(open_interest)

    plt.figure(figsize=(12, 6))
    plt.plot(x_axis['Put'], y_axis['Put'], label='Puts')
    plt.plot(x_axis['Call'], y_axis['Call'], label='Calls')

    plt.title('Open Interest for Calls and Puts Over Time for the Last Expiry')
    plt.xlabel('Date')
    plt.ylabel('Open Interest')
    plt.legend(['Put', 'Call']) 

    plt.savefig("Graphs/task7.png", dpi = 200)
    plt.close() 

    # task 8 
    print("-" * 25, "Task 8", "-" * 25)
    start_date = '2023-07-04'
    end_date = '2023-07-11'

    backtesting_query = """
        select date, time, option_type, tick_price from optiontickdata
        WHERE date >= :start_date AND date <= :end_date
        ORDER BY date, time; 
    """

    result = conn.execute(text(backtesting_query), {'start_date': start_date, 'end_date': end_date})
    result = result.fetchall()

    df = pd.DataFrame(result, columns=['date', 'time', 'option_type', 'price'])
    df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
    df.drop(columns=['time'], inplace=True)

    trade = []

    for i in range(len(df)):
        row = df.iloc[i]
        current_time = row['datetime']
        current_price = float(row['price'])

        if row['option_type'] == 'Call':
            end_time = current_time + timedelta(minutes=10)
            j = i + 1

            while j < len(df) and df.iloc[j]['datetime'] <= end_time:
                if df.iloc[j]['price'] >= 1.05 * current_price:
                    buy_time = df.iloc[j]['datetime']
                    buy_price = float(df.iloc[j]['price'])

                    # Find sell price
                    k = j + 1
                    sell_price = None
                    while k < len(df) and df.iloc[k]['datetime'].date() == buy_time.date():
                        if float(df.iloc[k]['price']) <= 0.97 * buy_price:
                            sell_price = float(df.iloc[k]['price'])
                            sell_time = df.iloc[k]['datetime']
                            break
                        k += 1

                    if sell_price is None:
                        sell_price = float(df.iloc[k - 1]['price'])
                        sell_time = df.iloc[k - 1]['datetime']

                    trade.append({
                        'buy_time': buy_time,
                        'buy_price': buy_price,
                        'sell_time': df.iloc[k]['datetime'],
                        'sell_price': sell_price,
                        'profit': sell_price - buy_price
                    })

                    j = k 

                j += 1

    trade_df = pd.DataFrame(trade)
    if not trade_df.empty:
        trade_df["cumulative_profit"] = trade_df['profit'].cumsum()

    total_profit = trade_df.iloc[-1]["cumulative_profit"]
    print(f"Total Profit: {total_profit}")
    
    # task 9
    print("-" * 25, "Task 9", "-" * 25)
    plt.figure(figsize=(12, 6))
    plt.plot(trade_df["sell_time"], trade_df["cumulative_profit"], label='Cumulative Profit')

    plt.title('Cumulative Profit vs Sell Time')
    plt.xlabel('Sell Time')
    plt.ylabel('Cumulative Profit')

    plt.grid(True)
    plt.savefig("Graphs/task9.png", dpi = 200)
    plt.close() 

    # Commit changes and close the connection
    conn.commit()
    conn.close()
