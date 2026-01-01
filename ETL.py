import os
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    print(message)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + "," + message + "\n")


def extract(df_list, file_location):
    log_progress("Beginning Extraction process...")
    for index, filename in enumerate(os.listdir(file_location)):
        try:
            filepath = os.path.join(file_location, filename)
            df_list[index] = pd.read_csv(filepath)
            log_progress(f"Successfully loaded {filename} into df_list[{index}]")

        except FileNotFoundError as e:
            log_progress(f"File not found: {filename}. Error: {e}")
            exit()
        except pd.errors.EmptyDataError as e:
            log_progress(f"Empty data in file: {filename}. Error: {e}")
            exit()
        except pd.errors.ParserError as e:
            log_progress(f"Parsing error in file: {filename}. Error: {e}")
            exit()
        except Exception as e:
            log_progress(f"An error occurred while processing file: {filename}. Error: {e}")
            exit()
    log_progress("Extraction process complete.")


def transform(df_list):
    log_progress("Beginning Transformation process...")
    for index, df in enumerate(df_list):
        try:
            df_long = pd.melt(df,
                              id_vars=['RegionID', 'SizeRank', 'RegionName',
                                       'RegionType', 'StateName', 'State',
                                       'Metro', 'StateCodeFIPS', 'MunicipalCodeFIPS'],
                              var_name='Date',
                              value_name='Value')

            df_long['Date'] = pd.to_datetime(df_long['Date'], format='%Y-%m-%d')
            df_long['Value'] = df_long['Value'].round(2)

            rename_dict = {
                'RegionID': 'region_id',
                'RegionName': 'region_name',
                'RegionType': 'region_type',
                'State': 'state',
                'Metro': 'metro',
                'StateCodeFIPS': 'state_code_fips',
                'MunicipalCodeFIPS': 'municipal_code_fips',
                'Date': 'date',
                'Value': 'value'}

            df_long.rename(columns=rename_dict, inplace=True)
            df_long['county'] = df_long['region_name'] + ", " + df_long['state']
            df_list[index] = df_long
            df_long['bedrooms'] = pd.NA

        except Exception as e:
            log_progress(f"An error occurred while transforming DataFrame at index {index}: {e}")
            exit()
    log_progress("Transformation process complete.")


def concatenate(df_list):
    log_progress("Beginning concatenation process...")
    try:

        combined_df = df_list[0].copy()
        combined_df['bedrooms'] = 1

        for i in range(1, len(df_list)):

            # error checking
            if df_list[i].empty:
                raise ValueError(f"DataFrame at index {i} is empty and cannot be merged.")

            # concatenating process
            df_list[i]['bedrooms'] = i + 1
            combined_df = pd.concat([combined_df, df_list[i]], ignore_index=True)

        log_progress("Data concatenation process complete.")
        return combined_df

    except ValueError as ve:
        log_progress(f"ValueError: {ve}")
    except TypeError as te:
        log_progress(f"TypeError: {te}")
    except Exception as e:
        log_progress(f"An error occurred during concatenation: {e}")
    exit()


def split_dataframe(combined_df):
    log_progress("Splitting dataframe...")
    try:
        required_region_columns = ['region_id', 'state', 'state_code_fips', 'municipal_code_fips', 'county']
        required_homevalue_columns = ['region_id', 'date', 'bedrooms', 'value']

        missing_region_columns = [col for col in required_region_columns if col not in combined_df.columns]
        missing_homevalue_columns = [col for col in required_homevalue_columns if col not in combined_df.columns]

        if missing_region_columns:
            raise ValueError(f"Missing columns for regions table: {missing_region_columns}")
        if missing_homevalue_columns:
            raise ValueError(f"Missing columns for homevalues table: {missing_homevalue_columns}")

        regions = combined_df[['region_id', 'state', 'state_code_fips', 'municipal_code_fips', 'county']].drop_duplicates()
        home_values = combined_df[['region_id', 'date', 'bedrooms', 'value']]

        if regions.empty:
            raise ValueError("The regions DataFrame is empty. Nothing to load.")
        if home_values.empty:
            raise ValueError("The home_values DataFrame is empty. Nothing to load.")

        log_progress("Dataframe splitting complete.")
        return regions, home_values

    except ValueError as ve:
        log_progress(f"ValueError: {ve}")
        exit()
    except Exception as e:
        log_progress(f"An error occurred: {e}")
        exit()


def null_heatmap(df):
    plt.figure(figsize=(10, 8))
    sns.heatmap(df.isnull(), cbar=False, cmap='viridis', yticklabels=False)
    plt.title('Null values')
    plt.show()


def output_table(df, count, title):
    df_head = df.head(count)
    df_head.to_csv(title, index=False)


def connect(file_path):

    log_progress("Beginning Connection process...")

    # Gathering credentials
    config = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if line and '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
                else:
                    log_progress(f"Invalid line in config file: '{line}'")
    except FileNotFoundError:
        log_progress(f"Error: The configuration file '{file_path}' was not found.")
        exit()
    except ValueError as e:
        log_progress(f"Error in configuration file format: {e}")
        exit()

    # connecting to db server
    db_username = config.get('db_username')
    db_password = config.get('db_password')
    db_host = config.get('db_host')
    db_port = config.get('db_port')
    db_name = config.get('db_name')

    try:
        engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
    except SQLAlchemyError as err:
        log_progress(f"Error: '{err}'")
        exit()

    log_progress("Successfully connected to SQL server.")
    return engine.connect()


def test_connection(connection):
    log_progress("Testing SQL server connection...")
    try:
        connection.execute(text("SELECT 1"))
    except SQLAlchemyError as e:
        log_progress(f"Connection to SQL server failed: {e}")
        exit()


def create_tables(connection):
    log_progress("Creating SQL tables...")
    queries = [
        """
        CREATE TABLE IF NOT EXISTS regions (
            region_id INT PRIMARY KEY,
            state VARCHAR(255),
            state_code_fips INT,
            municipal_code_fips INT,
            county VARCHAR(255)
        );""",

        """
        CREATE TABLE IF NOT EXISTS home_values (
            region_id INT,
            date DATE,
            value FLOAT,
            bedrooms INT,
            PRIMARY KEY (region_id, date, bedrooms),
            FOREIGN KEY (region_id) REFERENCES regions(region_id)
        );"""]


    try:
        for query in queries:
            connection.execute(text(query))
        log_progress("SQL Tables created successfully.")
    except SQLAlchemyError as e:
        log_progress(f"SQLAlchemyError: {e}")
        exit()
    except Exception as e:
        log_progress(f"An error occurred: {e}")
        exit()


def load(df, table_name, connection):
    log_progress(f"Loading data into {table_name}...")
    try:
        df.to_sql(table_name, connection, if_exists='append', index=False)
        log_progress(f"Data loaded into table '{table_name}' successfully.")
    except ValueError as ve:
        log_progress(f"ValueError: {ve}")
        exit()
    except SQLAlchemyError as se:
        log_progress(f"SQLAlchemyError: {se}")
        exit()
    except Exception as e:
        log_progress(f"An error occurred: {e}")
        exit()


def commit_data(connection):
    log_progress("Committing changes to SQL server...")
    try:
        connection.commit()
        log_progress("Changes committed to SQL server successfully...")
    except SQLAlchemyError as e:
        log_progress(f"Commit failed: {e}")
        rollback_data(connection)
        exit()
    except Exception as e:
        log_progress(f"An error occurred during commit: {e}")
        rollback_data(connection)
        exit()


def rollback_data(connection):
    try:
        connection.rollback()
        log_progress("Transaction rolled back successfully.")
    except SQLAlchemyError as e:
        log_progress(f"Rollback failed: {e}")
        exit()
    except Exception as e:
        log_progress(f"An error occurred during rollback: {e}")
        exit()


def main():
    df_list = []
    for _ in range(5):
        df_list.append(pd.DataFrame())

    # Extracting data files
    extract(df_list, "data/")

    # Transforming data
    transform(df_list)
    combined_df = concatenate(df_list)
    regions, home_values = split_dataframe(combined_df)
    null_heatmap(home_values)
    answer = input("Clean null values? (Y/N)")
    if answer in {'Y', 'y'}:
        log_progress(f"Sum before dropping null values: {home_values['value'].sum()}")
        home_values_cleaned = home_values.dropna(subset=['value'])
        log_progress(f"Sum after dropping null values: {home_values_cleaned['value'].sum()}")
        null_heatmap(home_values_cleaned)

    # Output sample of results to .csv file
    output_table(regions, 1000, 'regions')
    output_table(home_values_cleaned, 1000, 'home_values')
    answer = input("Commit dataframe results? (Y/N)")
    if answer not in {'Y', 'y'}:
        exit()

    # Connecting to SQL server
    connection = connect("db_config.txt")
    test_connection(connection)

    # Create tables
    create_tables(connection)

    # Loading data into database
    load(regions, 'regions', connection)
    load(home_values_cleaned, 'home_values', connection)

    # Commit changes
    commit_data(connection)

    # Verify data integrity
    log_progress(f"Home_values df # of rows: {len(home_values_cleaned)}")
    log_progress(f"regions df # of rows: {len(regions)}")
    log_progress(f"home_values value sum: {home_values_cleaned['value'].sum()}")


if __name__ == '__main__':
    main()