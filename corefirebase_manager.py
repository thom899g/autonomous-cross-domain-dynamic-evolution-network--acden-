"""
Firebase Firestore Manager for ACDEN
Central state management with real-time capabilities and automatic reconnection
"""
import json
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore, exceptions
from google.cloud.firestore_v1 import Client as FirestoreClient

logger = logging.getLogger(__name__)


class FirebaseManager:
    """Singleton Firebase Firestore manager with automatic error recovery"""
    
    _instance: Optional['FirebaseManager'] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._app = None
            self._db: Optional[FirestoreClient] = None
            self._credentials_path: Optional[Path] = None
            self._initialized = True
            logger.debug("FirebaseManager initialized")
    
    def initialize(self, credentials_path: Path) -> bool:
        """
        Initialize Firebase connection with robust error handling
        
        Args:
            credentials_path: Path to Firebase service account JSON file
            
        Returns:
            bool: True if initialization successful, False otherwise
            
        Raises:
            FileNotFoundError: If credentials file doesn't exist
            ValueError: If credentials are invalid
            FirebaseError: For Firebase-specific errors
        """
        try:
            # Validate credentials file exists
            if not credentials_path.exists():
                raise FileNotFoundError(
                    f"Firebase credentials file not found at {credentials_path}"
                )
            
            # Load and validate JSON
            with open(credentials_path, 'r') as f:
                cred_data = json.load(f)
                required_keys = {'type', 'project_id', 'private_key_id', 'private_key', 'client_email'}
                if not all(key in cred_data for key in required_keys):
                    raise ValueError("Invalid Firebase credentials format")
            
            self._credentials_path = credentials_path
            
            # Initialize or reinitialize app
            if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
                cred = credentials.Certificate(str(credentials_path))
                self._app = firebase_admin.initialize_app(cred)
                logger.info(f"Firebase app initialized for project: {cred_data.get('project_id')}")
            else:
                self._app = firebase_admin.get_app()
                logger.debug("Using existing Firebase app")
            
            # Initialize Firestore client
            self._db = firestore.client(self._app)
            
            # Test connection
            test_doc = self._db.collection('system_health').document('connection_test')
            test_doc.set({
                'timestamp': datetime.utcnow(),
                'status': 'connected',
                'test': True
            }, merge=True)
            
            logger.info("Firebase Firestore connection established and tested")
            return True
            
        except FileNotFoundError as e:
            logger.error(f"Credentials file error: {e}")
            raise
        except ValueError as e:
            logger.error(f"Credentials validation error: {e}")
            raise
        except exceptions.FirebaseError as e:
            logger.error(f"Firebase initialization error: {e}")
            self._db = None
            raise
        except Exception as e:
            logger.error(f"Unexpected Firebase initialization error: {e}")
            self._db = None
            raise
    
    @property
    def db(self) -> FirestoreClient:
        """Get Firestore client with connection validation"""
        if self._db is None:
            if self._credentials_path:
                self.initialize(self._credentials_path)
            else:
                raise ConnectionError("Firebase not initialized. Call initialize() first.")
        return self._db
    
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check of Firebase connection"""
        try:
            # Test write operation
            test_ref = self.db.collection('system_health').document('heartbeat')
            test_data = {
                'timestamp': datetime.utcnow(),
                'status': 'healthy',
                'latency_ms': None
            }
            
            import time
            start = time.time()
            test_ref.set(test_data, merge=True)
            latency = (time.time() - start) * 1000
            test_data['latency_ms'] = round(latency, 2)
            
            # Test read operation
            doc = test_ref.get()
            if not doc.exists:
                raise ConnectionError("Health check write/read failed")
            
            return {
                'status': 'healthy',
                'latency_ms': latency,
                'timestamp': datetime.utcnow(),
                'project_id': self._app.project_id if self._app else None
            }
            
        except Exception as e:
            logger.error(f"Firebase health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow()
            }
    
    def batch_operation(self, operations: List[Dict[str, Any]]) -> bool:
        """
        Execute batch operations with automatic retry
        
        Args:
            operations: List of dicts with keys 'type' (set/update/delete), 
                       'collection', 'document', 'data' (for set/update)
        
        Returns:
            bool: True if all operations successful
            
        Raises:
            ValueError: For invalid operation types
        """
        try:
            batch = self.db.batch()
            
            for op in operations:
                op_type = op.get('type')
                collection = op.get('collection')
                document = op.get('document')
                data = op.get('data', {})
                
                ref = self.db.collection(collection).document(document)