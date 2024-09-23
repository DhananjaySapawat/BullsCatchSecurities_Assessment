# BullsCatchSecurities Technical Assessment README

## Overview
This project generates graphs from data stored in a PostgreSQL database. Please ensure you have the necessary dependencies installed.

## Setup

1. **Configure Database Connection:**
   Edit the `.env` file with your PostgreSQL database parameters:
   ```plaintext
   # Database connection parameters
   db_user = your_username
   db_password = your_password
   db_host = your_host
   db_port = your_port
   db_name = your_database_name
   ```

2. **Install Dependencies:**
   All dependencies are listed in `req.txt`. You can install them using:
   ```bash
   pip install -r req.txt
   ```

## Running the Code
To run the code and generate the graphs, execute:
```bash
python main.py
```

## Output
Graphs will be generated and saved in the `graphs` folder. The program will also create the database and run all tasks required for graph generation.
