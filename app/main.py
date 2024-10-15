import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from typing import List, Optional
from bson import ObjectId

app = FastAPI()

client = MongoClient(os.environ['DATABASE_URL'])
db = client.geonames
collection = db.geonames

class GeoName(BaseModel):
    geonameid: int
    name: str
    latitude: float
    longitude: float
    country_code: str

class GeoQueryByIDs(BaseModel):
    geonameids: List[int]

class GeoQueryByLocation(BaseModel):
    locations: List[dict]  # Each dict will have latitude and longitude

@app.get("/")
def read_root():
    return {"message": "Welcome to the GeoNames API"}

@app.get("/geoname/{geonameid}", response_model=GeoName)
def get_geoname(geonameid: int):
    result = collection.find_one({"geonameid": geonameid})
    if result is None:
        raise HTTPException(status_code=404, detail="GeoName not found")
    return result

@app.get("/search/", response_model=List[GeoName])
def search_geonames(name: str, limit: int = 10):
    results = collection.find({"$text": {"$search": name}}).limit(limit)
    return list(results)

@app.post("/geonames/by_ids", response_model=List[GeoName])
def get_geonames_by_ids(query: GeoQueryByIDs):
    results = collection.find({"geonameid": {"$in": query.geonameids}})
    geonames = list(results)
    
    if not geonames:
        raise HTTPException(status_code=404, detail="No GeoNames found for the given IDs")
    
    return geonames

@app.post("/geonames/by_locations", response_model=List[GeoName])
def get_geonames_by_locations(query: GeoQueryByLocation):
    # We use a list comprehension to build a query with OR conditions for each location
    location_queries = [{"latitude": location["latitude"], "longitude": location["longitude"]} for location in query.locations]
    
    results = collection.find({"$or": location_queries})
    geonames = list(results)
    
    if not geonames:
        raise HTTPException(status_code=404, detail="No GeoNames found for the given locations")
    
    return geonames