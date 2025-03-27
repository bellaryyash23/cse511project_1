import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time
import os


class DataLoader:

    def __init__(self, uri, user, password):
        """
        Connect to the Neo4j database and other init steps
        
        Args:
            uri (str): URI of the Neo4j database
            user (str): Username of the Neo4j database
            password (str): Password of the Neo4j database
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()


    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.driver.close()


    # Define a function to create nodes and relationships in the graph
    def load_transform_file(self, file_path):
        """
        Load the parquet file and transform it into a csv file
        Then load the csv file into neo4j
        """
        try:
            # 1. Set up import directory - more robust path handling
            import_dir = "/var/lib/neo4j/import"
            os.makedirs(import_dir, exist_ok=True, mode=0o777)
        
            # 2. Read and filter data
            trips = pq.read_table(file_path).to_pandas()
        
            # Required columns
            trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]
        
            # Bronx filter (temporarily add debug output)
            bronx_locations = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259]
        
            print(f"Total trips before filtering: {len(trips)}")
            trips = trips[
                trips['PULocationID'].isin(bronx_locations) & 
                trips['DOLocationID'].isin(bronx_locations) &
                (trips['trip_distance'] > 0.1) &
                (trips['fare_amount'] > 2.5)
            ]
            print(f"Total trips after filtering: {len(trips)}")
        
            if trips.empty:
                raise ValueError("No trips remaining after filtering - check your filters")

            # 3. Convert datetime
            trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])
            trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])
        
            # 4. Save CSV with fixed filename
            csv_filename = "trips.csv"
            save_loc = os.path.join(import_dir, csv_filename)
            trips.to_csv(save_loc, index=False)
        
            # Verify file was created
            if not os.path.exists(save_loc):
                raise FileNotFoundError(f"Failed to create CSV at {save_loc}")
            print(f"CSV created successfully: {save_loc} ({os.path.getsize(save_loc)} bytes)")

            # 5. Load into Neo4j with absolute file path
            with self.driver.session() as session:
                query = """
                LOAD CSV WITH HEADERS FROM $csv_path AS row
                MERGE (pickup:Location {name: toInteger(row.PULocationID)})
                MERGE (dropoff:Location {name: toInteger(row.DOLocationID)})
                CREATE (pickup)-[:TRIP {
                    distance: toFloat(row.trip_distance),
                    fare: toFloat(row.fare_amount),
                    pickup_dt: datetime(row.tpep_pickup_datetime),
                    dropoff_dt: datetime(row.tpep_dropoff_datetime)
                }]->(dropoff)
                """
                # Use absolute file path in URI format
                session.run(query, csv_path=f"file://{save_loc}")
                print("Data loaded into Neo4j successfully!")

        except Exception as e:
            print(f"Error in load_transform_file: {str(e)}")
            raise


def main():

    total_attempts = 10
    attempt = 0

    # The database takes some time to starup!
    # Try to connect to the database 10 times
    while attempt < total_attempts:
        try:
            data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "project1phase1")
            data_loader.load_transform_file("yellow_tripdata_2022-03.parquet")
            data_loader.close()
            
            attempt = total_attempts

        except Exception as e:
            print(f"(Attempt {attempt+1}/{total_attempts}) Error: ", e)
            attempt += 1
            time.sleep(10)


if __name__ == "__main__":
    main()

