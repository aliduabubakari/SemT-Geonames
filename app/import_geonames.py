import os
import urllib.request
import zipfile
import io
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure, BulkWriteError

# Download GeoNames data
url = "http://download.geonames.org/export/dump/allCountries.zip"
print("Downloading GeoNames data...")
response = urllib.request.urlopen(url)
zip_content = io.BytesIO(response.read())

# Connect to MongoDB
print("Connecting to MongoDB...")
try:
    client = pymongo.MongoClient(os.environ['DATABASE_URL'])
    client.admin.command('ismaster')
    print("MongoDB connection successful")
except ConnectionFailure:
    print("Server not available")
    exit(1)
except OperationFailure:
    print("Authentication failed")
    exit(1)

db = client.geonames
collection = db.geonames

# Drop existing collection and indexes
print("Dropping existing collection...")
collection.drop()

# Import data
print("Importing data...")
try:
    with zipfile.ZipFile(zip_content) as zip_file:
        with zip_file.open('allCountries.txt') as f:
            batch = []
            for line in io.TextIOWrapper(f, encoding='utf-8'):
                fields = line.strip().split('\t')
                geoname = {
                    'geonameid': int(fields[0]),
                    'name': fields[1],
                    'asciiname': fields[2],
                    'alternatenames': fields[3].split(','),
                    'latitude': float(fields[4]),
                    'longitude': float(fields[5]),
                    'feature_class': fields[6],
                    'feature_code': fields[7],
                    'country_code': fields[8],
                    'cc2': fields[9].split(','),
                    'admin1_code': fields[10],
                    'admin2_code': fields[11],
                    'admin3_code': fields[12],
                    'admin4_code': fields[13],
                    'population': int(fields[14]) if fields[14] else None,
                    'elevation': int(fields[15]) if fields[15] else None,
                    'dem': int(fields[16]) if fields[16] else None,
                    'timezone': fields[17],
                    'modification_date': fields[18]
                }
                batch.append(geoname)
                if len(batch) == 1000:
                    try:
                        collection.insert_many(batch, ordered=False)
                    except BulkWriteError as bwe:
                        print(f"Encountered {len(bwe.details['writeErrors'])} errors. Continuing...")
                    batch = []
                    print("Processed 1000 records...")
    
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
            except BulkWriteError as bwe:
                print(f"Encountered {len(bwe.details['writeErrors'])} errors. Continuing...")

    # Handle duplicates
    print("Handling duplicates...")
    pipeline = [
        {"$group": {
            "_id": "$geonameid",
            "dups": {"$addToSet": "$_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {
            "count": {"$gt": 1}
        }},
        {"$sort": {"_id": 1}}  # Sort to ensure consistent ordering
    ]
    
    batch_size = 1000
    cursor = collection.aggregate(pipeline, allowDiskUse=True)
    
    total_removed = 0
    while True:
        batch = list(cursor.next() for _ in range(batch_size) if cursor.alive)
        if not batch:
            break
        
        for duplicate in batch:
            # Keep the first document, remove the rest
            to_remove = duplicate["dups"][1:]
            result = collection.delete_many({"_id": {"$in": to_remove}})
            total_removed += result.deleted_count
        
        print(f"Processed {batch_size} groups of duplicates...")
    
    print(f"Removed {total_removed} duplicate entries")

    # Create indexes
    print("Creating indexes...")
    collection.create_index([('name', pymongo.TEXT)])
    collection.create_index([('geonameid', pymongo.ASCENDING)], unique=True)
    collection.create_index([('latitude', pymongo.ASCENDING), ('longitude', pymongo.ASCENDING)])

    print("Import completed")
except Exception as e:
    print(f"An error occurred: {e}")
    exit(1)