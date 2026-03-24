"""
TikTok Content Scraper with MongoDB + PostgreSQL Support
========================================================
- MongoDB: เก็บ metadata (like, share, view, comment, etc.)
- PostgreSQL: เก็บ progress tracking
"""

import os
from pathlib import Path
import time
from datetime import timedelta
import statistics
import json

from .src.logger import logger
from .src.scraper_functions.base_scraper import BaseScraper

# Initialize html scraper
base_scraper = BaseScraper()


class TT_Content_Scraper_Cloud:
    """TikTok Content Scraper with MongoDB and PostgreSQL support"""
    
    def __init__(self,
                 # PostgreSQL settings (for progress tracking)
                 postgres_host: str = "localhost",
                 postgres_port: int = 5432,
                 postgres_database: str = "tiktok_scraper",
                 postgres_user: str = "postgres",
                 postgres_password: str = "postgres",
                 postgres_connection_string: str = None,
                 
                 # MongoDB settings (for metadata storage)
                 mongodb_connection_string: str = "mongodb://localhost:27017",
                 mongodb_database: str = "tiktok_scraper",
                 
                 # Scraper settings
                 wait_time: float = 0.35,
                 clear_console: bool = False,
                 
                 # Fallback to file storage
                 use_file_fallback: bool = False,
                 output_files_fp: str = "data/"):
        """
        Initialize scraper with cloud database support
        
        Parameters:
        -----------
        postgres_* : PostgreSQL connection settings
        mongodb_* : MongoDB connection settings
        wait_time : float
            Wait time between requests
        use_file_fallback : bool
            If True, also save to local files as backup
        """
        
        # Initialize PostgreSQL tracker
        from .src.postgres_tracker import PostgresObjectTracker
        self.tracker = PostgresObjectTracker(
            host=postgres_host,
            port=postgres_port,
            database=postgres_database,
            user=postgres_user,
            password=postgres_password,
            connection_string=postgres_connection_string
        )
        
        # Initialize MongoDB storage
        from .src.mongodb_storage import MongoDBStorage
        self.storage = MongoDBStorage(
            connection_string=mongodb_connection_string,
            database_name=mongodb_database
        )
        
        # Settings
        self.WAIT_TIME = wait_time
        self.clear_console = clear_console
        self.use_file_fallback = use_file_fallback
        self.output_files_fp = output_files_fp
        
        # Stats
        self.iter_times = []
        self.ITER_TIME = 0
        self.iterations = 0
        self.repeated_error = 0
        self.n_scraped_total = 0
        self.n_errors_total = 0
        self.n_pending = 0
        self.n_retry = 0
        self.n_total = 0
        self.mean_iter_time = 0
        self.queue_eta = "0:00:00"
        
        # Create output folder if using file fallback
        if use_file_fallback:
            Path(output_files_fp).mkdir(parents=True, exist_ok=True)
        
        logger.info("Scraper Initialized (MongoDB + PostgreSQL)")
        logger.info(f"  PostgreSQL: {postgres_database}")
        logger.info(f"  MongoDB: {mongodb_database}")
    
    def add_objects(self, ids: list, title: str = None, type: str = "content"):
        """Add objects to tracking queue"""
        self.tracker.add_objects(ids, title=title, type=type)
    
    def scrape_pending(self, only_content: bool = False, only_users: bool = False, scrape_files: bool = False):
        """Scrape all pending objects"""
        
        if only_content:
            seed_type = "content"
        elif only_users:
            seed_type = "user"
        else:
            seed_type = "all"
        
        while True:
            seedlist = self.tracker.get_pending_objects(type=seed_type, limit=100)
            assert len(seedlist) > 0, f"No more pending objects of type {seed_type} to scrape"
            
            for self.iterations, seed in enumerate(seedlist.items()):
                start = time.time()
                
                id = seed[0]
                obj_type = seed[1]["type"]
                
                if self.clear_console:
                    os.system('clear')
                
                logger.info(f"Scraping ID: {id}")
                self._logging_queue_progress(type=seed_type)
                
                if obj_type == "user":
                    self._user_action_protocol(id)
                elif obj_type == "content":
                    self._content_action_protocol(id, scrape_files)
                
                # Measure time
                stop = time.time()
                self.ITER_TIME = stop - start
                wait_time_left = max(0, self.WAIT_TIME - self.ITER_TIME)
                self.ITER_TIME = self.ITER_TIME + wait_time_left
                
                logger.info("Continuing with next ID...\n\n--------")
                
                time.sleep(wait_time_left)
                self.repeated_error = 0
    
    def _user_action_protocol(self, id: str):
        """Scrape and store user data"""
        try:
            user_data = base_scraper.scrape_user(id)
            
            # Save to MongoDB
            self.storage.save_user_metadata(id, user_data)
            
            # Save to file as backup
            if self.use_file_fallback:
                filepath = os.path.join(self.output_files_fp, "user_metadata/", f"{id}.json")
                Path(self.output_files_fp, "user_metadata/").mkdir(parents=True, exist_ok=True)
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(user_data, f, ensure_ascii=False, indent=4)
            
            self.tracker.mark_completed(id, f"mongodb:{id}")
            self.n_scraped_total += 1
            
        except Exception as e:
            logger.warning(f"Error scraping user {id}: {e}")
            self.tracker.mark_error(id, str(e))
            self.n_errors_total += 1
    
    def _content_action_protocol(self, id: str, scrape_files: bool):
        """Scrape and store content data"""
        try:
            sorted_metadata, link_to_binaries = base_scraper.scrape_metadata(id)
        except KeyError as e:
            logger.warning(f"ID {id} did not lead to any metadata - KeyError {e}")
            self.tracker.mark_error(id, str(e))
            self.n_errors_total += 1
            self.n_pending -= 1
            return None
        
        # Optionally scrape files
        if scrape_files:
            try:
                self._scrape_and_save_files(id, link_to_binaries, sorted_metadata)
            except ConnectionError as e:
                logger.warning(f"ID {id} did not lead to any downloadable files: {e}")
        
        # Save metadata to MongoDB
        self.storage.save_content_metadata(id, sorted_metadata)
        
        # Save to file as backup
        if self.use_file_fallback:
            filepath = os.path.join(self.output_files_fp, "content_metadata/", f"{id}.json")
            Path(self.output_files_fp, "content_metadata/").mkdir(parents=True, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(sorted_metadata, f, ensure_ascii=False, indent=4)
        
        self.tracker.mark_completed(id, f"mongodb:{id}")
        self.n_scraped_total += 1
        self.n_pending -= 1
    
    def _scrape_and_save_files(self, id: str, link_to_binaries: dict, metadata: dict):
        """Scrape and save binary files"""
        Path(self.output_files_fp, "content_files/").mkdir(parents=True, exist_ok=True)
        
        binaries = base_scraper.scrape_binaries(link_to_binaries)
        
        if binaries["mp4"]:
            metadata["file_metadata"]["is_slide"] = False
            filepath = Path(self.output_files_fp, "content_files/", f"tiktok_video_{id}.mp4")
            with open(filepath, 'wb') as f:
                f.write(binaries["mp4"])
        elif binaries["jpegs"]:
            metadata["file_metadata"]["is_slide"] = True
            for i, jpeg in enumerate(binaries["jpegs"]):
                filepath = Path(self.output_files_fp, "content_files/", f"tiktok_picture_{id}_{i}.jpeg")
                with open(filepath, 'wb') as f:
                    f.write(jpeg)
            if binaries["mp3"]:
                filepath = Path(self.output_files_fp, "content_files/", f"tiktok_audio_{id}.mp3")
                with open(filepath, 'wb') as f:
                    f.write(binaries["mp3"])
    
    def _logging_queue_progress(self, type: str):
        """Log progress"""
        if self.iterations == 0:
            stats = self.tracker.get_stats(type)
            self.n_scraped_total = stats["completed"]
            self.n_errors_total = stats["errors"]
            self.n_pending = stats["pending"]
            self.n_retry = stats["retry"]
            self.n_total = self.n_scraped_total + self.n_errors_total + self.n_pending + self.n_retry
        
        # Calculate ETA
        self.iter_times.insert(0, self.ITER_TIME)
        if len(self.iter_times) > 100:
            self.iter_times.pop(0)
        
        if self.iterations % 15 == 0 and self.iterations < 2_000:
            self.mean_iter_time = statistics.mean(self.iter_times) if self.iter_times else 0
            self.queue_eta = str(timedelta(seconds=int(self.n_pending * self.mean_iter_time)))
        
        if self.n_total > 0 or self.n_scraped_total > 0:
            logger.info(f"Scraped objects ► {(self.n_scraped_total + self.n_errors_total):,} / {self.n_total:,}")
            logger.info(f"...minus errors ► {self.n_scraped_total:,}")
        
        if self.repeated_error > 0:
            logger.info(f"Errors in a row ► {self.repeated_error}")
        
        logger.info(f"Iteration time ► {round(self.ITER_TIME, 2)} sec.")
        logger.info(f"......averaged ► {round(self.mean_iter_time, 2)} sec.")
        logger.info(f"ETA ► {self.queue_eta}\n↓↓↓")
    
    def get_stats(self) -> dict:
        """Get combined stats from both databases"""
        postgres_stats = self.tracker.get_stats()
        mongodb_stats = self.storage.get_stats()
        
        return {
            "tracking": postgres_stats,
            "storage": mongodb_stats
        }
    
    def close(self):
        """Close all connections"""
        self.tracker.close()
        self.storage.close()
        logger.info("All connections closed")
