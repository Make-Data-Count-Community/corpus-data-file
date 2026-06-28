#!/usr/bin/env python3
"""
Simple FastAPI web service for querying Crossref database
Run with: uvicorn crossref_web_api:app --reload
"""

from fastapi import FastAPI, HTTPException
from crossref_importer import CrossrefDB
from pydantic import BaseModel

DB_PATH = "/Volumes/Storage/crossref/crossref-api.db"

app = FastAPI(
    title="Simple Crossref API",
    description="Simple API for looking up academic papers by DOI",
    version="1.0.0"
)

db = CrossrefDB(DB_PATH)

class PaperResponse(BaseModel):
    doi: str
    published_date: str
    publisher: str
    journal: str


@app.get("/paper/{doi:path}", response_model=PaperResponse)
async def get_paper(doi: str):
    """
    Get paper information by DOI
    
    Args:
        doi: The DOI to fetch (e.g., "10.1042/bj20130269")
    
    Returns:
        Paper information with doi, published_date, publisher, and journal
    """
    try:
        result = db.search_by_doi(doi)
        if not result:
            raise HTTPException(status_code=404, detail="Paper not found")
        
        response = PaperResponse(
            doi=result.get('doi', ''),
            published_date=result.get('published_date', ''),
            publisher=result.get('publisher', ''),
            journal=result.get('journal', '')
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Simple database connectivity check
        with db.get_connection() as conn:
            conn.execute("SELECT COUNT(*) FROM papers").fetchone()
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {str(e)}")
