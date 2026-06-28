#!/usr/bin/env python3
"""
Crossref API for querying academic papers from SQLite database
Supports external drive storage for large datasets (200GB+)
"""

import sqlite3
import json
import gzip
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CrossrefDB:
    """Database manager for Crossref academic papers"""
    
    def __init__(self, db_path: str):
        """
        Initialize database connection
        
        Args:
            db_path: Path to SQLite database (can be on external drive)
                    e.g., "/Volumes/MyDrive/crossref.db" or "D:/crossref.db"
        """
        self.db_path = db_path
        self.ensure_database_exists()
        
    def ensure_database_exists(self):
        """Create database and tables if they don't exist"""
        with self.get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS papers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doi TEXT UNIQUE NOT NULL,
                    journal TEXT,
                    publisher TEXT,
                    published_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    local_indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_doi_primary ON papers(doi)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_journal ON papers(journal)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_publisher ON papers(publisher)')
            
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
        finally:
            conn.close()
    
    def import_jsonl_files(self, data_directory: str, batch_size: int = 10000):
        """
        Import all .jsonl.gz files from directory into database
        
        Args:
            data_directory: Directory containing .jsonl.gz files
            batch_size: Number of records to insert at once (default: 10000 for faster imports)
        """
        data_path = Path(data_directory)
        gz_files = list(data_path.glob("*.jsonl.gz"))
        
        if not gz_files:
            logger.error(f"No .jsonl.gz files found in {data_directory}")
            return
        
        logger.info(f"Found {len(gz_files)} files to process")
        
        total_imported = 0
        batch_data = []
        
        with self.get_connection() as conn:
            # Optimize SQLite for bulk inserts
            conn.execute('PRAGMA journal_mode = WAL')
            conn.execute('PRAGMA synchronous = NORMAL')
            conn.execute('PRAGMA cache_size = 100000')
            conn.execute('PRAGMA temp_store = MEMORY')
            
            # Begin transaction for the entire import
            conn.execute('BEGIN TRANSACTION')
            
            for gz_file in gz_files:
                logger.info(f"Processing {gz_file.name}")
                
                try:
                    with gzip.open(gz_file, 'rt', encoding='utf-8') as f:
                        for line_num, line in enumerate(f, 1):
                            try:
                                record = json.loads(line.strip())
                                
                                # Extract key fields
                                doi = record.get('DOI', '')
                                journal = self._extract_journal(record)
                                publisher = record.get('publisher', '')
                                published_date = self._extract_date(record.get('published', {}))
                                
                                batch_data.append((
                                    doi, journal, publisher, published_date
                                ))
                                
                                if len(batch_data) >= batch_size:
                                    self._insert_batch(conn, batch_data, commit=False)
                                    total_imported += len(batch_data)
                                    batch_data = []
                                    
                                    if total_imported % 50000 == 0:
                                        logger.info(f"Imported {total_imported} records so far...")
                                        conn.commit()  # Commit every 50k records
                                        conn.execute('BEGIN TRANSACTION')
                                
                            except json.JSONDecodeError:
                                logger.warning(f"Invalid JSON in {gz_file.name}, line {line_num}")
                            except Exception as e:
                                logger.error(f"Error processing record in {gz_file.name}, line {line_num}: {e}")
                
                except Exception as e:
                    logger.error(f"Error processing file {gz_file.name}: {e}")
            
            if batch_data:
                self._insert_batch(conn, batch_data, commit=False)
                total_imported += len(batch_data)
            
            conn.commit()
        
        logger.info(f"Import complete! Total records imported: {total_imported}")
    
    def _insert_batch(self, conn, batch_data, commit=True):
        """Insert a batch of records into database"""
        conn.executemany('''
            INSERT OR REPLACE INTO papers (
                doi, journal, publisher,published_date
            ) VALUES (?, ?, ?, ?)
        ''', batch_data)
        if commit:
            conn.commit()
    
    def _extract_journal(self, record: Dict) -> str:
        """Extract journal name from record"""
        journal = record.get('container-title', [])
        if isinstance(journal, list) and journal:
            return journal[0]
        return str(journal) if journal else ''
    
    def _extract_date(self, date_obj: Dict) -> str:
        """Extract date string from Crossref date object"""
        if not date_obj:
            return ''
        
        date_time = date_obj.get('date-time', '')
        if date_time:
            return date_time
        
        date_parts = date_obj.get('date-parts', [])
        if date_parts and len(date_parts) > 0 and len(date_parts[0]) > 0:
            parts = date_parts[0]
            if len(parts) >= 3:
                return f"{parts[0]}-{parts[1]:02d}-{parts[2]:02d}"
            elif len(parts) >= 2:
                return f"{parts[0]}-{parts[1]:02d}"
            else:
                return str(parts[0])
        
        return ''
    
    def get_paper_by_doi(self, doi: str) -> Optional[Dict]:
        """
        Get a paper by DOI (primary lookup method)
        
        Args:
            doi: The DOI to search for (e.g., "10.1006/jmcc.2000.1342")
            
        Returns:
            Paper data if found, None otherwise
        """
        with self.get_connection() as conn:
            result = conn.execute(
                'SELECT * FROM papers WHERE doi = ?', (doi,)
            ).fetchone()
            return dict(result) if result else None
    
    def search_by_doi(self, doi: str) -> Optional[Dict]:
        """Search for paper by DOI (alias for get_paper_by_doi)"""
        return self.get_paper_by_doi(doi)
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        with self.get_connection() as conn:
            total_papers = conn.execute('SELECT COUNT(*) FROM papers').fetchone()[0]
            
            top_publishers = conn.execute('''
                SELECT publisher, COUNT(*) as count 
                FROM papers 
                WHERE publisher != '' 
                GROUP BY publisher 
                ORDER BY count DESC 
                LIMIT 10
            ''').fetchall()
            
            return {
                'total_papers': total_papers,
                'top_publishers': [{'publisher': row[0], 'count': row[1]} for row in top_publishers],
            }

def main():
    """Example usage of the CrossrefDB class"""
    
    # Configuration - UPDATE THESE PATHS FOR YOUR SETUP
    EXTERNAL_DRIVE_PATH = "/Volumes/Storage/crossref"  # macOS example
    # EXTERNAL_DRIVE_PATH = "D:"  # Windows example
    # EXTERNAL_DRIVE_PATH = "/mnt/external"  # Linux example
    
    DB_PATH = f"{EXTERNAL_DRIVE_PATH}/crossref-api.db"
    DATA_DIRECTORY = "/Volumes/Storage/crossref/data"  # Directory with .jsonl.gz files
    
    db = CrossrefDB(DB_PATH)
    
    # Check if database is empty and needs import
    stats = db.get_stats()
    if stats['total_papers'] == 0:
        print("Database is empty. Starting import...")
        print("This may take several hours...")
        print("Using optimized batch size of 10,000 records for faster imports...")
        db.import_jsonl_files(DATA_DIRECTORY, batch_size=10000)
    else:
        print(f"Database contains {stats['total_papers']} papers")


if __name__ == "__main__":
    main()
