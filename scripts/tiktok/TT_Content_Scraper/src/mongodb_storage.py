"""
MongoDB Storage for TikTok Content Metadata
===========================================
เก็บ metadata ของ video ลง MongoDB แทนการเขียนไฟล์ JSON
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from datetime import datetime
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger('TTCS.MongoDB')


class MongoDBStorage:
    """Store TikTok content metadata in MongoDB"""
    
    def __init__(self, 
                 connection_string: str = "mongodb://localhost:27017",
                 database_name: str = "tiktok_scraper",
                 content_collection: str = "content_metadata",
                 user_collection: str = "user_metadata"):
        """
        Initialize MongoDB connection
        
        Parameters:
        -----------
        connection_string : str
            MongoDB connection string (e.g., "mongodb://localhost:27017" or MongoDB Atlas URI)
        database_name : str
            Name of the database
        content_collection : str
            Collection name for content metadata
        user_collection : str
            Collection name for user metadata
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.content_collection_name = content_collection
        self.user_collection_name = user_collection
        
        self.client = None
        self.db = None
        self.content_collection = None
        self.user_collection = None
        
        self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            # Test connection
            self.client.admin.command('ping')
            
            self.db = self.client[self.database_name]
            self.content_collection = self.db[self.content_collection_name]
            self.user_collection = self.db[self.user_collection_name]
            
            # Create indexes
            self._create_indexes()
            
            logger.info(f"Connected to MongoDB: {self.database_name}")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """Create indexes for better query performance"""
        # Content collection indexes
        self.content_collection.create_index("video_id", unique=True)
        self.content_collection.create_index("created_at")
        self.content_collection.create_index("author.unique_id")
        self.content_collection.create_index("hashtags.name")
        
        # User collection indexes
        self.user_collection.create_index("unique_id", unique=True)
        self.user_collection.create_index("created_at")
        
        logger.info("MongoDB indexes created")
    
    def save_content_metadata(self, video_id: str, metadata: Dict[str, Any]) -> bool:
        """
        Save content metadata to MongoDB
        
        Parameters:
        -----------
        video_id : str
            TikTok video ID
        metadata : dict
            Video metadata dictionary
        
        Returns:
        --------
        bool : Success status
        """
        try:
            document = {
                "video_id": video_id,
                "metadata": metadata,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            # Flatten some common fields for easier querying
            if "engagement" in metadata:
                document["likes"] = metadata["engagement"].get("digg_count")
                document["comments"] = metadata["engagement"].get("comment_count")
                document["shares"] = metadata["engagement"].get("share_count")
                document["plays"] = metadata["engagement"].get("play_count")
            
            if "author" in metadata:
                document["author"] = metadata["author"]
            
            if "hashtags" in metadata.get("content", {}):
                document["hashtags"] = metadata["content"]["hashtags"]
            
            # Upsert (insert or update)
            self.content_collection.update_one(
                {"video_id": video_id},
                {"$set": document},
                upsert=True
            )
            
            logger.debug(f"Saved content metadata for video_id: {video_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving content metadata: {e}")
            return False
    
    def save_user_metadata(self, username: str, metadata: Dict[str, Any]) -> bool:
        """
        Save user metadata to MongoDB
        
        Parameters:
        -----------
        username : str
            TikTok username
        metadata : dict
            User metadata dictionary
        
        Returns:
        --------
        bool : Success status
        """
        try:
            document = {
                "unique_id": username,
                "metadata": metadata,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            # Flatten common fields
            if "user" in metadata:
                user_info = metadata["user"]
                document["nickname"] = user_info.get("nickname")
                document["signature"] = user_info.get("signature")
                document["verified"] = user_info.get("verified")
            
            if "stats" in metadata:
                stats = metadata["stats"]
                document["followers"] = stats.get("followerCount")
                document["following"] = stats.get("followingCount")
                document["likes"] = stats.get("heart")
                document["video_count"] = stats.get("videoCount")
            
            # Upsert
            self.user_collection.update_one(
                {"unique_id": username},
                {"$set": document},
                upsert=True
            )
            
            logger.debug(f"Saved user metadata for: {username}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving user metadata: {e}")
            return False
    
    def get_content_metadata(self, video_id: str) -> Optional[Dict]:
        """Get content metadata by video ID"""
        return self.content_collection.find_one({"video_id": video_id})
    
    def get_user_metadata(self, username: str) -> Optional[Dict]:
        """Get user metadata by username"""
        return self.user_collection.find_one({"unique_id": username})
    
    def get_all_content(self, limit: int = 100, skip: int = 0) -> list:
        """Get all content metadata with pagination"""
        return list(self.content_collection.find().skip(skip).limit(limit))
    
    def get_content_by_hashtag(self, hashtag: str, limit: int = 100) -> list:
        """Get content by hashtag"""
        return list(self.content_collection.find(
            {"hashtags.name": hashtag}
        ).limit(limit))
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        return {
            "content_count": self.content_collection.count_documents({}),
            "user_count": self.user_collection.count_documents({}),
            "database": self.database_name
        }
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


# Example usage
if __name__ == "__main__":
    # Test connection
    storage = MongoDBStorage(
        connection_string="mongodb://localhost:27017",
        database_name="tiktok_scraper"
    )
    
    print(f"Stats: {storage.get_stats()}")
    storage.close()
