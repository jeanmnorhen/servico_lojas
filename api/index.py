import os
import json
import uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, auth, firestore

# --- SQLAlchemy and GeoAlchemy2 Imports ---
from sqlalchemy import create_engine, Column, String, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from geoalchemy2 import Geography
from geoalchemy2.shape import to_shape

# --- Kafka Imports ---
from confluent_kafka import Producer

from flask_cors import CORS

# --- Configuração do Flask ---
app = Flask(__name__)
CORS(app)

# --- Configuração do Firebase ---
db = None
try:
    firebase_admin_sdk_json = os.environ.get('FIREBASE_ADMIN_SDK_JSON')
    if firebase_admin_sdk_json:
        cred = credentials.Certificate(json.loads(firebase_admin_sdk_json))
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("Firebase inicializado com sucesso.")
    else:
        # Fallback para arquivo local em desenvolvimento
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        cred_path = os.path.join(project_root, 'firebase-adminsdk.json')
        if os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            db = firestore.client()
            print("Firebase inicializado com arquivo local.")
        else:
            print("Variável de ambiente FIREBASE_ADMIN_SDK_JSON não encontrada e arquivo local firebase-adminsdk.json ausente.")
except Exception as e:
    print(f"Erro ao inicializar Firebase: {e}")

# --- Configuração do PostgreSQL (PostGIS) ---
db_session = None
engine = None
try:
    db_url = os.environ.get('POSTGRES_URL')
    if db_url:
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        print("Conexão com PostgreSQL (PostGIS) estabelecida com sucesso.")
    else:
        print("Variável de ambiente POSTGRES_URL não encontrada.")
except Exception as e:
    print(f"Erro ao conectar com PostgreSQL: {e}")

# --- Definição do Modelo de Dados Geoespacial ---
Base = declarative_base()

class StoreLocation(Base):
    __tablename__ = 'store_locations'
    store_id = Column(String, primary_key=True)
    location = Column(Geography(geometry_type='POINT', srid=4326), nullable=False)

# --- Função para criar a tabela no banco ---
def init_db():
    if engine:
        try:
            Base.metadata.create_all(engine)
            print("Tabela 'store_locations' verificada/criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar tabela 'store_locations': {e}")

# --- Configuração do Kafka Producer ---
producer = None
try:
    kafka_conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get('KAFKA_API_KEY'),
        'sasl.password': os.environ.get('KAFKA_API_SECRET')
    }
    if kafka_conf['bootstrap.servers']:
        producer = Producer(kafka_conf)
        print("Produtor Kafka inicializado com sucesso.")
    else:
        print("Variáveis de ambiente do Kafka não encontradas.")
except Exception as e:
    print(f"Erro ao inicializar Produtor Kafka: {e}")

def delivery_report(err, msg):
    if err is not None:
        print(f'Falha ao entregar mensagem Kafka: {err}')
    else:
        print(f'Mensagem Kafka entregue em {msg.topic()} [{msg.partition()}]')

def publish_event(topic, event_type, store_id, data, changes=None):
    if not producer:
        print("Produtor Kafka não está inicializado. Evento não publicado.")
        return
    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": store_id,
        "data": data,
        "source_service": "servico-lojas"
    }
    if changes:
        event["changes"] = changes
    try:
        event_value = json.dumps(event, default=str)
        producer.produce(topic, key=store_id, value=event_value, callback=delivery_report)
        producer.poll(0)
        print(f"Evento '{event_type}' para a loja {store_id} publicado no tópico {topic}.")
    except Exception as e:
        print(f"Erro ao publicar evento Kafka: {e}")

# --- Rotas da API ---

@app.route("/api/stores", methods=["POST"])
def create_store():
    """Cria uma nova loja, salvando dados no Firestore e PostGIS."""
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    # 1. Autenticação
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    id_token = auth_header.split('Bearer ')[1]
    try:
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    # 2. Validação dos Dados
    store_data = request.get_json()
    if not store_data or not store_data.get('name'):
        return jsonify({"error": "Store name is required"}), 400
    
    store_id = str(uuid.uuid4())

    # Prepara dados para o Firestore (sem localização)
    firestore_data = store_data.copy()
    location_data = firestore_data.pop('location', None) # Remove location para salvar separadamente
    firestore_data['owner_uid'] = uid
    firestore_data['created_at'] = firestore.SERVER_TIMESTAMP
    firestore_data['updated_at'] = firestore.SERVER_TIMESTAMP

    try:
        # Transação: Salva no PostGIS e depois no Firestore
        if location_data and 'latitude' in location_data and 'longitude' in location_data:
            lat = location_data['latitude']
            lon = location_data['longitude']
            wkt_point = f'POINT({lon} {lat})'
            new_location = StoreLocation(store_id=store_id, location=wkt_point)
            db_session.add(new_location)

        db.collection('stores').document(store_id).set(firestore_data)
        
        db_session.commit()

        # Publica evento
        publish_event('eventos_lojas', 'StoreCreated', store_id, store_data)
        return jsonify({"message": "Store created successfully", "storeId": store_id}), 201

    except SQLAlchemyError as e:
        db_session.rollback()
        return jsonify({"error": f"Erro no banco de dados geoespacial: {e}"}), 500
    except Exception as e:
        db_session.rollback() # Garante rollback em caso de falha no Firestore
        return jsonify({"error": f"Erro ao criar loja: {e}"}), 500

@app.route('/api/stores/<store_id>', methods=['GET'])
def get_store(store_id):
    """Retorna os dados de uma loja, incluindo sua localização geoespacial."""
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    try:
        # Pega dados do Firestore
        store_doc = db.collection('stores').document(store_id).get()
        if not store_doc.exists:
            return jsonify({"error": "Loja não encontrada."}), 404
        store_data = store_doc.to_dict()
        store_data['id'] = store_doc.id

        # Pega dados do PostGIS
        location_record = db_session.query(StoreLocation).filter_by(store_id=store_id).first()
        if location_record:
            point = to_shape(location_record.location)
            store_data['location'] = {'latitude': point.y, 'longitude': point.x}

        return jsonify(store_data), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao buscar loja: {e}"}), 500

@app.route('/api/stores/<store_id>', methods=['PUT'])
def update_store(store_id):
    """Atualiza os dados de uma loja, incluindo sua localização geoespacial."""
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    # 1. Autenticação
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    id_token = auth_header.split('Bearer ')[1]
    try:
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    update_data = request.get_json()
    if not update_data:
        return jsonify({"error": "Dados para atualização são obrigatórios."}), 400

    store_ref = db.collection('stores').document(store_id)
    
    try:
        store_doc = store_ref.get()
        if not store_doc.exists:
            return jsonify({"error": "Loja não encontrada."}), 404
        
        # 2. Autorização: Verifica se o usuário autenticado é o dono da loja
        if store_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to update this store"}), 403

        # Prepara dados para o Firestore (sem localização)
        firestore_data = update_data.copy()
        location_data = firestore_data.pop('location', None)
        firestore_data['updated_at'] = firestore.SERVER_TIMESTAMP

        # Atualiza PostGIS se a localização for fornecida
        if location_data and 'latitude' in location_data and 'longitude' in location_data:
            lat = location_data['latitude']
            lon = location_data['longitude']
            wkt_point = f'POINT({lon} {lat})'
            
            location_record = db_session.query(StoreLocation).filter_by(store_id=store_id).first()
            if location_record:
                location_record.location = wkt_point
            else:
                new_location = StoreLocation(store_id=store_id, location=wkt_point)
                db_session.add(new_location)

        # Atualiza Firestore
        if firestore_data:
            store_ref.update(firestore_data)

        db_session.commit()

        # Publica evento
        publish_event('eventos_lojas', 'StoreUpdated', store_id, update_data)
        return jsonify({"message": "Loja atualizada com sucesso.", "storeId": store_id}), 200

    except SQLAlchemyError as e:
        db_session.rollback()
        return jsonify({"error": f"Erro no banco de dados geoespacial: {e}"}), 500
    except Exception as e:
        db_session.rollback()
        return jsonify({"error": f"Erro ao atualizar loja: {e}"}), 500

@app.route('/api/stores/<store_id>', methods=['DELETE'])
def delete_store(store_id):
    """Deleta uma loja do Firestore e PostGIS."""
    if not db or not db_session:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    # 1. Autenticação
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    id_token = auth_header.split('Bearer ')[1]
    try:
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    store_ref = db.collection('stores').document(store_id)

    try:
        store_doc = store_ref.get()
        if not store_doc.exists:
            return jsonify({"error": "Loja não encontrada."}), 404
        
        # 2. Autorização: Verifica se o usuário autenticado é o dono da loja
        if store_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to delete this store"}), 403

        # Deleta do PostGIS
        location_record = db_session.query(StoreLocation).filter_by(store_id=store_id).first()
        if location_record:
            db_session.delete(location_record)

        # Deleta do Firestore
        store_ref.delete()

        db_session.commit()

        # Publica evento
        publish_event('eventos_lojas', 'StoreDeleted', store_id, {"store_id": store_id})
        return '', 204

    except SQLAlchemyError as e:
        db_session.rollback()
        return jsonify({"error": f"Erro no banco de dados geoespacial: {e}"}), 500
    except Exception as e:
        db_session.rollback()
        return jsonify({"error": f"Erro ao deletar loja: {e}"}), 500

# --- Health Check (para Vercel) ---
@app.route('/api/health', methods=['GET'])
def health_check():
    # Verifica status do PostgreSQL
    pg_status = "error"
    if db_session:
        try:
            from sqlalchemy import text
            db_session.execute(text('SELECT 1'))
            pg_status = "ok"
        except Exception:
            pass # pg_status remains 'error'

    status = {
        "firestore": "ok" if db else "error",
        "kafka_producer": "ok" if producer else "error",
        "postgresql": pg_status
    }
    http_status = 200 if all(s == "ok" for s in status.values()) else 503
    return jsonify(status), http_status

# --- Inicialização ---
init_db()

if __name__ == '__main__':
    app.run(debug=True)
