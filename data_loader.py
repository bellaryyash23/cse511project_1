import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import os

class DataLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()

    def close(self):
        self.driver.close()

    def load_transform_file(self, file_path):
        # Read the parquet file from /data directory
        trips = pq.read_table(file_path)
        trips = trips.to_pandas()

        # Filter columns and clean data
        trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]

        # Bronx location filter
        bronx = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259]
        
        trips = trips[
            trips['PULocationID'].isin(bronx) & 
            trips['DOLocationID'].isin(bronx) &
            (trips['trip_distance'] > 0.1) &
            (trips['fare_amount'] > 2.5)
        ]

        # Convert datetime
        trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        # Save CSV to Neo4j's import directory
        csv_filename = "trips.csv"
        save_loc = f"/var/lib/neo4j/import/{csv_filename}"
        trips.to_csv(save_loc, index=False)

        # Load data into Neo4j
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
            session.run(query, csv_path=f"file:///{csv_filename}")

def main():
    data_loader = DataLoader("bolt://localhost:7687", "neo4j", "project1phase1")
    data_loader.load_transform_file("/data/yellow_tripdata.parquet")
    data_loader.close()

if __name__ == "__main__":
    main()