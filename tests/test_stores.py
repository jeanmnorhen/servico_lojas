import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

# Import a mock for the Point object that to_shape would create
class MockPoint:
    def __init__(self, x, y):
        self.x = x
        self.y = y

# Mock a StoreLocation record that the SQLAlchemy query would return
class MockStoreLocation:
    def __init__(self, store_id, location_str):
        self.store_id = store_id
        self.location = location_str # The WKT string

# Mock all external dependencies for all tests
@pytest.fixture(autouse=True)
def mock_all_dependencies():
    # 1. Mock Firebase
    mock_fs_doc = MagicMock()
    mock_fs_doc.exists = True
    mock_fs_doc.id = "test_store_123"
    # Firestore data no longer contains location
    mock_fs_doc.to_dict.return_value = {
        "name": "Test Store",
        "owner_uid": "test_owner_uid",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc)
    }

    # 2. Mock PostGIS/SQLAlchemy session
    mock_sql_session = MagicMock()
    # The query chain: .query().filter_by().first()
    mock_location_record = MockStoreLocation('test_store_123', 'POINT(-46.6 -23.5)')
    mock_sql_session.query.return_value.filter_by.return_value.first.return_value = mock_location_record

    # 3. Mock Kafka Producer
    mock_kafka_producer_instance = MagicMock()

    # 4. Mock geoalchemy2.shape.to_shape
    # This function converts the DB location object to a shapely Point
    mock_to_shape = MagicMock(return_value=MockPoint(x=-46.6, y=-23.5))

    # Apply all mocks using patch
    with patch('api.index.db', MagicMock()) as mock_db, \
         patch('api.index.db_session', mock_sql_session), \
         patch('api.index.producer', mock_kafka_producer_instance), \
         patch('api.index.to_shape', mock_to_shape), \
         patch('api.index.publish_event') as mock_publish_event: 

        # Configure the mock for Firestore document retrieval
        mock_db.collection.return_value.document.return_value.get.return_value = mock_fs_doc
        
        # Yield to allow tests to run with these mocks
        yield {
            "db": mock_db,
            "db_session": mock_sql_session,
            "producer": mock_kafka_producer_instance,
            "to_shape": mock_to_shape,
            "publish_event": mock_publish_event
        }

@pytest.fixture
def client():
    """A test client for the app."""
    from api.index import app
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

# --- Test Cases ---

def test_create_store_with_location(client, mock_all_dependencies):
    """Test creating a store with location data."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_store_creation"
    headers = {"Authorization": f"Bearer {fake_token}"}
    mock_all_dependencies["db"].collection.return_value.add.return_value = (MagicMock(), MagicMock(id="new_store_id"))
    mock_all_dependencies["db"].collection.return_value.document.return_value.set.return_value = None # For .set() method
    
    with patch('api.index.auth.verify_id_token', return_value={'uid': user_uid}):
        store_data = {
            "name": "New Store",
            "address": "123 Main St",
            "location": {"latitude": -23.5, "longitude": -46.6}
        }
        response = client.post('/api/stores', headers=headers, json=store_data)

        assert response.status_code == 201
        assert "storeId" in response.json
        
        # Assert that PostGIS session was used
        mock_all_dependencies["db_session"].add.assert_called_once()
        mock_all_dependencies["db_session"].commit.assert_called_once()

        # Assert that Firestore was used
        mock_all_dependencies["db"].collection.return_value.document.assert_called_once()
        mock_all_dependencies["db"].collection.return_value.document.return_value.set.assert_called_once()

        # Assert that Kafka event was published
        mock_all_dependencies["publish_event"].assert_called_once()
        args, kwargs = mock_all_dependencies["publish_event"].call_args
        assert args[1] == 'StoreCreated'

def test_create_store_without_location(client, mock_all_dependencies):
    """Test creating a store without location data."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_store_creation"
    headers = {"Authorization": f"Bearer {fake_token}"}
    mock_all_dependencies["db"].collection.return_value.add.return_value = (MagicMock(), MagicMock(id="new_store_id"))
    mock_all_dependencies["db"].collection.return_value.document.return_value.set.return_value = None # For .set() method

    with patch('api.index.auth.verify_id_token', return_value={'uid': user_uid}):
        store_data = {"name": "No Location Store", "address": "456 Oak Ave"}
        response = client.post('/api/stores', headers=headers, json=store_data)

        assert response.status_code == 201
        
        # Assert that PostGIS session was NOT used to add data, but commit is still called
        mock_all_dependencies["db_session"].add.assert_not_called()
        mock_all_dependencies["db_session"].commit.assert_called_once()

        # Assert that Firestore was used
        mock_all_dependencies["db"].collection.return_value.document.assert_called_once()
        mock_all_dependencies["db"].collection.return_value.document.return_value.set.assert_called_once()

def test_get_store_with_location(client, mock_all_dependencies):
    """Test getting a store who has a location in PostGIS."""
    response = client.get('/api/stores/test_store_123')

    assert response.status_code == 200
    assert response.json['id'] == 'test_store_123'
    assert 'location' in response.json
    assert response.json['location']['latitude'] == -23.5
    assert response.json['location']['longitude'] == -46.6
    
    # Assert that PostGIS was queried
    mock_all_dependencies["db_session"].query.assert_called_once()
    # Assert that to_shape was called to parse the location
    mock_all_dependencies["to_shape"].assert_called_once()

def test_get_store_without_location(client, mock_all_dependencies):
    """Test getting a store who does not have a location in PostGIS."""
    # Setup mock to return None for the location query
    mock_all_dependencies["db_session"].query.return_value.filter_by.return_value.first.return_value = None
    
    response = client.get('/api/stores/test_store_123')

    assert response.status_code == 200
    assert 'location' not in response.json # The key should be absent
    
    mock_all_dependencies["db_session"].query.assert_called_once()
    mock_all_dependencies["to_shape"].assert_not_called() # Should not be called if no record is found

def test_get_store_not_found(client, mock_all_dependencies):
    """Test getting a store that does not exist in Firestore."""
    mock_all_dependencies["db"].collection.return_value.document.return_value.get.return_value.exists = False
    
    response = client.get('/api/stores/non_existent_store')
    assert response.status_code == 404

def test_update_store_location(client, mock_all_dependencies):
    """Test updating a store's location."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_store_update"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    with patch('api.index.auth.verify_id_token', return_value={'uid': user_uid}):
        update_data = {"location": {"latitude": -10.0, "longitude": -20.0}}
        response = client.put('/api/stores/test_store_123', headers=headers, json=update_data)

        assert response.status_code == 200
        
        # Assert that the location record object was modified and commit was called
        location_record = mock_all_dependencies["db_session"].query.return_value.filter_by.return_value.first.return_value
        assert location_record.location == 'POINT(-20.0 -10.0)'
        mock_all_dependencies["db_session"].commit.assert_called_once()

        # Assert event was published
        mock_all_dependencies["publish_event"].assert_called_once()
        args, kwargs = mock_all_dependencies["publish_event"].call_args
        assert args[1] == 'StoreUpdated'
        assert args[2] == 'test_store_123'
        assert args[3] == update_data

def test_delete_store(client, mock_all_dependencies):
    """Test deleting a store."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_store_delete"
    headers = {"Authorization": f"Bearer {fake_token}"}

    with patch('api.index.auth.verify_id_token', return_value={'uid': user_uid}):
        response = client.delete('/api/stores/test_store_123', headers=headers)

        assert response.status_code == 204
        
        # Assert that delete was called on the session
        mock_sql_session = mock_all_dependencies["db_session"]
        location_record = mock_sql_session.query.return_value.filter_by.return_value.first.return_value
        mock_sql_session.delete.assert_called_once_with(location_record)
        mock_sql_session.commit.assert_called_once()

        # Assert that delete was called on Firestore
        mock_all_dependencies["db"].collection.return_value.document.return_value.delete.assert_called_once()

        # Assert event was published
        mock_all_dependencies["publish_event"].assert_called_once()

def test_health_check_all_ok(client, mock_all_dependencies):
    """Test health check when all services are up."""
    response = client.get('/api/health')
    assert response.status_code == 200
    assert response.json == {
        "firestore": "ok",
        "kafka_producer": "ok",
        "postgresql": "ok"
    }

def test_health_check_pg_error(client, mock_all_dependencies):
    """Test health check when PostgreSQL is down."""
    # Simulate a DB error
    from sqlalchemy import text
    mock_all_dependencies["db_session"].execute.side_effect = Exception("Connection failed")
    
    response = client.get('/api/health')
    assert response.status_code == 503
    assert response.json["postgresql"] == "error"

