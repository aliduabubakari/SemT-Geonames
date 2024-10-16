import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from typing import List, Optional
from bson import ObjectId
from fuzzywuzzy import fuzz
from geopy.distance import distance

print(f"DATABASE_URL: {os.environ.get('DATABASE_URL', 'Not set')}")

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
    population: int
    feature_class: str
    feature_code: str
    admin1_code: str
    confidence_score: Optional[float] = None

class GeoQueryByIDs(BaseModel):
    geonameids: List[int]

class GeoQueryByLocation(BaseModel):
    locations: List[dict]  # Each dict will have latitude and longitude

class LocationQuery(BaseModel):
    name: str
    country_code: Optional[str] = None
    admin1_code: Optional[str] = None
    nearby_lat: Optional[float] = None
    nearby_lon: Optional[float] = None

@app.get("/")
def read_root():
    return {"message": "Welcome to the GeoNames API"}

@app.get("/geoname/{geonameid}", response_model=GeoName)
def get_geoname(geonameid: int):
    result = collection.find_one({"geonameid": geonameid})
    if result is None:
        raise HTTPException(status_code=404, detail="GeoName not found")
    return result

@app.post("/search/disambiguate", response_model=List[GeoName])
def search_and_disambiguate(query: LocationQuery, limit: int = 10):
    # Start with a text search
    text_search_query = {"$text": {"$search": query.name}}
    
    # Add filters if provided
    if query.country_code:
        text_search_query["country_code"] = query.country_code
    if query.admin1_code:
        text_search_query["admin1_code"] = query.admin1_code
    
    results = list(collection.find(text_search_query).limit(50))  # Get more results initially
    
    # Score and rank results
    scored_results = []
    for result in results:
        score = 0
        
        # Name similarity score (0-100)
        name_similarity = fuzz.ratio(query.name.lower(), result['name'].lower())
        score += name_similarity
        
        # Population score (0-50)
        pop_score = min(50, result.get('population', 0) / 100000)
        score += pop_score
        
        # Feature class/code score (0-30)
        if result.get('feature_class') == 'P':  # Populated place
            score += 30
        elif result.get('feature_class') == 'A':  # Administrative division
            score += 20
        
        # Proximity score if nearby coordinates provided (0-20)
        if query.nearby_lat and query.nearby_lon:
            dist = distance((query.nearby_lat, query.nearby_lon), (result['latitude'], result['longitude'])).km
            proximity_score = max(0, 20 - (dist / 10))  # 20 points for 0 km, 0 points for 200+ km
            score += proximity_score
        
        result['confidence_score'] = score / 2  # Normalize to 0-100 scale
        scored_results.append(result)
    
    # Sort by confidence score and return top results
    scored_results.sort(key=lambda x: x['confidence_score'], reverse=True)
    return scored_results[:limit]


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