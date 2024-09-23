import pandas as pd
import os
import re 
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, time, timedelta
import matplotlib.pyplot as plt

# Database connection parameters
db_user = 'postgres'
db_password = '12345678'
db_host = 'localhost'
db_port = '5432'       
db_name = 'test'

if __name__ == '__main__':

    # Create the engine
    connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    conn = engine.connect() 

    # task 8 
    start_date = '2023-07-04'
    end_date = '2023-07-11'
    start_date = '2024-01-02'
    end_date = '2024-02-11'

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

    # task 9
    if not trade_df.empty:
        trade_df["cumulative_profit"] = trade_df['profit'].cumsum()

    plt.figure(figsize=(12, 6))
    plt.plot(trade_df["sell_time"], trade_df["cumulative_profit"], label='Cumulative Profit')

    plt.title('Cumulative Profit vs Sell Time')
    plt.xlabel('Sell Time')
    plt.ylabel('Cumulative Profit')

    plt.grid(True)
    plt.savefig("Graphs/task9.png", dpi=200)

    # Commit changes and close the connection
    conn.commit()
    conn.close()
