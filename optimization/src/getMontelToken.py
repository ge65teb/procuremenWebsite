import psycopg2
import csv


def get_data_from_db(db_url, query):
    """
    Connect to the PostgreSQL database and execute the given query.

    Parameters:
    - db_url (str): Database connection URL.
    - query (str): SQL query to execute.

    Returns:
    - list of tuples: The query results.
    """
    try:
        # Establish connection using a context manager to ensure proper closure
        with psycopg2.connect(db_url) as conn:
            print("Connected to the database successfully.")

            # Create a cursor object using a context manager
            with conn.cursor() as cursor:
                # Execute the query
                cursor.execute(query)

                # Fetch all rows from the executed query
                rows = cursor.fetchall()

        # Return the fetched rows
        return rows

    except Exception as e:
        print(f"Database error: {e}")
        return []

def save_to_csv(filename, rows):
    """
    Save the given rows to a CSV file.

    Parameters:
    - filename (str): The name of the CSV file to save data to.
    - rows (list of tuples): The data rows to write to the CSV file.
    """
    try:
        # Open the file in write mode with newline parameter to handle line endings
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)

            # Write the rows to the CSV file
            writer.writerows(rows)

        print(f"Data successfully written to {filename}")

    except Exception as e:
        print(f"Error writing to CSV: {e}")

def main():
    """
    Main function to fetch data from the database and save it to a CSV file.
    """
    # Replace 'your_database_url_here' with your actual database URL or connection string
    DB_URL = 'postgresql://retool:hOc2JYpWU6wn@ep-summer-mode-114239.us-west-2.retooldb.com/retool?sslmode=require'

    # Define the SQL query to execute
    query = 'SELECT token FROM token'

    # Get data from the database
    rows = get_data_from_db(DB_URL, query)

    # Check if any data was returned
    if rows:
        # Save the data to a CSV file
        save_to_csv('/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/src/token.csv', rows)
    else:
        print("No data to save.")

if __name__ == '__main__':
    main()