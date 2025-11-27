from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect
import requests
import os
import json
from datetime import datetime

from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams,
    Distance,
    PointStruct,
    PayloadSchemaType,
    Filter,
    FieldCondition,
    MatchValue,
)
from google import genai
import uuid
from datetime import datetime, timezone
app = Flask(__name__)
app.secret_key = 'YOUR_SECRET_KEY'  # change in prod

# ========= OAuth for Cliq (unchanged, optional) =========
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

REDIRECT_URI = 'http://localhost:8000/oauth/callback'
AUTH_URL = 'https://accounts.zoho.com/oauth/v2/auth'
TOKEN_URL = 'https://accounts.zoho.com/oauth/v2/token'
SCOPE = 'ZohoCliq.Channels.READ ZohoCliq.Messages.READ ZohoCliq.Messages.CREATE'

ACCESS_TOKEN = None
REFRESH_TOKEN = None

# ========= Signals =========
SIGNALS_EVENT_URL = os.getenv('SIGNALS_EVENT_URL')

# ========= Catalyst Data Store =========
CATALYST_TOKEN = os.getenv("CATALYST_TOKEN")          # Zoho-oauthtoken with DataStore.row.CREATE,DataStore.row.READ
CATALYST_PROJECT_ID = os.getenv("CATALYST_PROJECT_ID")
CATALYST_ORG_ID = os.getenv("CATALYST_ORG_ID")
CONVERSATIONS_TABLE = "conversations"                # Table name (id 56037000000014013)

# ========= Qdrant / Gemini =========
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = "messages_vec"

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
genai_client = genai.Client()   # uses GEMINI_API_KEY env


# ---------- Embedding + Qdrant helpers ----------

def normalize_message_id(raw_id: str) -> str:
    # Deterministic UUID5 from the raw string
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))


def embed_text(text: str) -> list[float]:
    res = genai_client.models.embed_content(
        model="gemini-embedding-001",
        contents=text
    )
    return res.embeddings[0].values


def ensure_qdrant_collection(vector_dim: int):
    """Create collection and payload index if needed."""
    collections = qdrant.get_collections().collections
    if not any(c.name == QDRANT_COLLECTION for c in collections):
        qdrant.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),
        )
        print(f"✅ Created Qdrant collection {QDRANT_COLLECTION}")
    try:
        qdrant.create_payload_index(
            collection_name=QDRANT_COLLECTION,
            field_name="category",
            field_schema=PayloadSchemaType.KEYWORD,
        )
    except Exception:
        pass  # already exists


def classify_message(text: str) -> str:
    """Simple keyword-based classifier; swap with QuickML later."""
    t = text.lower()
    if any(w in t for w in ["error", "down", "timeout", "issue", "bug", "problem"]):
        return "incident"
    if any(w in t for w in ["fixed", "resolved", "restarted", "deployed", "solved"]):
        return "response"
    return "discussion"


# ---------- Data Store helpers ----------
def insert_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, message_text, category):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        print("⚠️ Catalyst config missing; skipping Data Store insert")
        return None

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "Content-Type": "application/json",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    body = [{
        "conversation_id": conversation_id,
        "message_id": message_id,
        "sender_id": sender_id,
        "time_stamp": timestamp_ms,   # bigint, not datetime string
        "message_text": message_text,
        "category": category,
    }]

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        print("DS insert status:", resp.status_code, resp.text)
        if resp.status_code == 201:
            return resp.json()[0].get("ROWID")
    except Exception as e:
        print("DS insert exception:", e)
    return None


# def insert_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, message_text, category):
#     """Insert one row into conversations table."""
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         print("⚠️ Catalyst config missing; skipping Data Store insert")
#         return None

#     ts_iso = datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat() + "Z"

#     url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "Content-Type": "application/json",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
#     body = [{
#         "conversation_id": conversation_id,
#         "message_id": message_id,
#         "sender_id": sender_id,
#         "time_stamp": ts_iso,
#         "message_text": message_text,
#         "category": category,
#     }]

#     try:
#         resp = requests.post(url, headers=headers, json=body, timeout=10)
#         print("DS insert status:", resp.status_code, resp.text)
#         if resp.status_code == 201:
#             return resp.json()[0].get("ROWID")
#     except Exception as e:
#         print("DS insert exception:", e)
#     return None


def fetch_by_message_ids(message_ids: list[str]):
    """Fetch messages from Data Store and filter by message_id."""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID and message_ids):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        resp = requests.get(f"{url}?max_rows=200", headers=headers, timeout=10)
        if resp.status_code != 200:
            print("DS fetch error:", resp.status_code, resp.text)
            return []
        rows = resp.json().get("data", [])
        return [r for r in rows if r.get("message_id") in message_ids]
    except Exception as e:
        print("DS fetch exception:", e)
        return []


# ---------- Full indexing pipeline ----------
def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
    """Classify, store in Data Store, then embed & index in Qdrant."""
    category = classify_message(message_text)
    print(f"📋 Category: {category}")

    # Store original Cliq message_id in Data Store
    row_id = insert_into_datastore(
        conversation_id, message_id, sender_id, timestamp_ms, message_text, category
    )

    emb = embed_text(message_text)
    ensure_qdrant_collection(len(emb))

    qdrant_id = normalize_message_id(message_id)

    point = PointStruct(
        id=qdrant_id,   # Qdrant-safe id
        vector=emb,
        payload={
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "category": category,
            "row_id": row_id,
            "message_id": message_id,  # keep original id in payload
        },
    )
    qdrant.upsert(QDRANT_COLLECTION, [point])
    print(f"✅ Indexed in Qdrant: cli_id={message_id}, qdrant_id={qdrant_id}")
# def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
#     """Classify, store in Data Store, then embed & index in Qdrant."""
#     category = classify_message(message_text)
#     print(f"📋 Category: {category}")

#     row_id = insert_into_datastore(
#         conversation_id, message_id, sender_id, timestamp_ms, message_text, category
#     )

#     emb = embed_text(message_text)
#     ensure_qdrant_collection(len(emb))

#     point = PointStruct(
#         id=message_id,
#         vector=emb,
#         payload={
#             "conversation_id": conversation_id,
#             "sender_id": sender_id,
#             "category": category,
#             "row_id": row_id,
#         },
#     )
#     qdrant.upsert(QDRANT_COLLECTION, [point])
#     print(f"✅ Indexed in Qdrant: {message_id}")


# ---------- Semantic search ----------

@app.route("/search", methods=["POST"])
def search():
    data = request.get_json(force=True)
    query = data.get("query", "")
    top_k = int(data.get("top_k", 5))
    category = data.get("category")  # optional

    if not query:
        return jsonify({"error": "query required"}), 400

    q_emb = embed_text(query)

    q_filter = None
    if category:
        q_filter = Filter(
            must=[FieldCondition(key="category", match=MatchValue(value=category))]
        )

    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=top_k,
    )

    # After hits = qdrant.search(...)
    ids = [h.payload.get("message_id") for h in hits if h.payload and h.payload.get("message_id")]
    rows = fetch_by_message_ids(ids)

    results = []
    for h in hits:
        orig_id = h.payload.get("message_id") if h.payload else None
        if not orig_id:
            continue
        row = next((r for r in rows if r.get("message_id") == orig_id), None)
        if not row:
            continue
        results.append({
            "message_id": orig_id,
            "score": h.score,
            "message_text": row.get("message_text"),
            "category": row.get("category"),
            "conversation_id": row.get("conversation_id"),
            "sender_id": row.get("sender_id"),
            "time_stamp": row.get("time_stamp"),
        })


    # ids = [str(h.id) for h in hits]
    # rows = fetch_by_message_ids(ids)

    # results = []
    # for h in hits:
    #     row = next((r for r in rows if r.get("message_id") == str(h.id)), None)
    #     if not row:
    #         continue
    #     results.append({
    #         "message_id": h.id,
    #         "score": h.score,
    #         "message_text": row.get("message_text"),
    #         "category": row.get("category"),
    #         "conversation_id": row.get("conversation_id"),
    #         "sender_id": row.get("sender_id"),
    #         "time_stamp": row.get("time_stamp"),
    #     })

    return jsonify({"query": query, "results": results})


# ========= OAuth endpoints (unchanged) =========

@app.route('/login')
def login():
    params = {
        'client_id': CLIENT_ID,
        'scope': SCOPE,
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI,
        'access_type': 'offline',
        'prompt': 'consent'
    }
    auth_request = requests.Request('GET', AUTH_URL, params=params).prepare()
    return redirect(auth_request.url)


@app.route('/oauth/callback')
def oauth_callback():
    global ACCESS_TOKEN, REFRESH_TOKEN
    code = request.args.get('code')
    if not code:
        return "Missing authorization code", 400

    data = {
        'code': code,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    }
    resp = requests.post(TOKEN_URL, data=data)
    token_json = resp.json()

    if 'access_token' not in token_json:
        return f"Failed to get access token: {token_json}", 400

    ACCESS_TOKEN = token_json.get('access_token')
    REFRESH_TOKEN = token_json.get('refresh_token')
    return "OAuth Successful!"


# ========= Producer: Deluge → Flask → Signals =========

@app.route('/bot/events', methods=['POST'])
def bot_events():
    form_data = request.form.to_dict()
    print("---- Received from Deluge ----")
    print(form_data)

    operation = form_data.get('operation')
    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    data_raw = form_data.get('data')

    inner_data = {
        "source": "zoho_cliq_bot",
        "operation": operation,
        "user": user_raw,
        "chat": chat_raw,
        "raw": data_raw
    }

    event_payload = {"data": inner_data}

    if SIGNALS_EVENT_URL:
        try:
            resp = requests.post(
                SIGNALS_EVENT_URL,
                headers={"Content-Type": "application/json"},
                json=event_payload
            )
            print("Signals response:", resp.status_code, resp.text)
        except Exception as e:
            print("Error publishing to Signals:", e)

    return jsonify({"status": "ok"})


# ========= Consumer: Signals → Flask → Data Store + Qdrant =========

@app.route('/signals/consume', methods=['POST'])
def signals_consume():
    payload = request.get_json()
    print("==== Signals delivered queued event ====")
    print(json.dumps(payload, indent=2))

    events = payload.get("events", [])
    if not events:
        return jsonify({"status": "no_events"}), 200

    event_obj = events[0]
    outer_data = event_obj.get("data", {})
    inner_data = outer_data.get("data", {})

    raw_str = inner_data.get("raw")
    user_str = inner_data.get("user")
    chat_str = inner_data.get("chat")

    if not (raw_str and user_str and chat_str):
        print("Missing raw/user/chat; skipping")
        return jsonify({"status": "ignored"}), 200

    try:
        raw = json.loads(raw_str)
        user = json.loads(user_str)
        chat = json.loads(chat_str)
    except Exception as e:
        print("Error parsing JSON fields:", e)
        return jsonify({"status": "parse_error"}), 200

    try:
        message_text = raw["message"]["content"]["text"]
        message_id = raw["message"]["id"]
        timestamp_ms = raw["time"]
    except KeyError as e:
        print("Missing field in raw:", e)
        return jsonify({"status": "bad_raw"}), 200

    sender_id = user.get("zoho_user_id") or user.get("id")
    conversation_id = chat.get("id")  # or channel_unique_name / channel_id

    print(f"📨 {message_text!r}")
    print(f"   conv={conversation_id}, sender={sender_id}, ts_ms={timestamp_ms}")

    index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)

    return jsonify({"status": "processed"})


if __name__ == '__main__':
    app.run(port=8000, debug=True)

# from dotenv import load_dotenv
# load_dotenv()

# from flask import Flask, request, jsonify, redirect
# import requests
# import os
# import json
# from datetime import datetime
# from qdrant_client import QdrantClient
# from qdrant_client.models import VectorParams, Distance, PointStruct, PayloadSchemaType, Filter, FieldCondition, MatchValue
# from google import genai

# app = Flask(__name__)
# app.secret_key = 'YOUR_SECRET_KEY'

# # ===== Zoho OAuth =====
# CLIENT_ID = os.getenv('CLIENT_ID')
# CLIENT_SECRET = os.getenv('CLIENT_SECRET')
# REDIRECT_URI = 'http://localhost:8000/oauth/callback'
# AUTH_URL = 'https://accounts.zoho.in/oauth/v2/auth'
# TOKEN_URL = 'https://accounts.zoho.in/oauth/v2/token'
# SCOPE = 'ZohoCliq.Channels.READ ZohoCliq.Messages.READ ZohoCliq.Messages.CREATE'

# ACCESS_TOKEN = None
# REFRESH_TOKEN = None

# # ===== Signals =====
# SIGNALS_EVENT_URL = os.getenv('SIGNALS_EVENT_URL')

# # ===== Catalyst Data Store =====
# CATALYST_TOKEN = os.getenv("CATALYST_TOKEN")
# CATALYST_PROJECT_ID = os.getenv("CATALYST_PROJECT_ID")
# CATALYST_ORG_ID = os.getenv("CATALYST_ORG_ID")
# CONVERSATIONS_TABLE_ID = "56037000000014013"  # Your table ID
# CONVERSATIONS_TABLE = "conversations"

# # ===== Qdrant =====
# QDRANT_URL = os.getenv("QDRANT_URL")
# QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
# QDRANT_COLLECTION = "messages_vec"

# # ===== Clients =====
# qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
# genai_client = genai.Client()


# # ========== EMBEDDING & QDRANT FUNCTIONS ==========

# def embed_text(text: str) -> list[float]:
#     """Generate embedding using Gemini"""
#     res = genai_client.models.embed_content(
#         model="gemini-embedding-001",
#         contents=text
#     )
#     return res.embeddings[0].values


# def ensure_qdrant_collection(vector_dim: int):
#     """Ensure Qdrant collection exists with payload index"""
#     collections = qdrant.get_collections().collections
#     if not any(c.name == QDRANT_COLLECTION for c in collections):
#         qdrant.create_collection(
#             collection_name=QDRANT_COLLECTION,
#             vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE)
#         )
#         print(f"✅ Created Qdrant collection: {QDRANT_COLLECTION}")
    
#     try:
#         qdrant.create_payload_index(
#             collection_name=QDRANT_COLLECTION,
#             field_name="category",
#             field_schema=PayloadSchemaType.KEYWORD
#         )
#     except Exception as e:
#         pass  # Index might already exist


# def classify_message(text: str) -> str:
#     """Simple keyword-based classification (replace with QuickML later)"""
#     text_lower = text.lower()
#     if any(word in text_lower for word in ['error', 'down', 'timeout', 'issue', 'problem', 'bug']):
#         return "incident"
#     elif any(word in text_lower for word in ['fixed', 'resolved', 'restarted', 'deployed', 'solved']):
#         return "response"
#     else:
#         return "discussion"


# # ========== DATA STORE FUNCTIONS ==========

# def insert_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, message_text, category):
#     """Insert message row into Catalyst Data Store"""
#     if not CATALYST_TOKEN or not CATALYST_PROJECT_ID:
#         print("⚠️ Catalyst config missing; skipping Data Store insert")
#         return None
    
#     ts_iso = datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat() + "Z"
    
#     url = f"https://api.catalyst.zoho.in/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "Content-Type": "application/json",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
    
#     body = [{
#         "conversation_id": conversation_id,
#         "message_id": message_id,
#         "sender_id": sender_id,
#         "time_stamp": ts_iso,
#         "message_text": message_text,
#         "category": category,
#     }]
    
#     try:
#         resp = requests.post(url, headers=headers, json=body, timeout=10)
#         print(f"✅ Data Store insert: {resp.status_code}")
#         if resp.status_code == 201:
#             result = resp.json()
#             return result[0].get("ROWID")  # Return ROWID for reference
#         else:
#             print(f"❌ Data Store error: {resp.text}")
#             return None
#     except Exception as e:
#         print(f"❌ Data Store exception: {e}")
#         return None


# def fetch_messages_by_ids(message_ids: list[str]):
#     """Fetch full message details from Data Store by message_ids"""
#     if not CATALYST_TOKEN or not CATALYST_PROJECT_ID or not message_ids:
#         return []
    
#     url = f"https://api.catalyst.zoho.in/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
    
#     # Fetch all rows (you can optimize with filters if Data Store supports it)
#     # For now, we fetch and filter in memory
#     try:
#         resp = requests.get(f"{url}?max_rows=100", headers=headers, timeout=10)
#         if resp.status_code == 200:
#             data = resp.json()
#             rows = data.get("data", [])
#             # Filter by message_ids
#             return [r for r in rows if r.get("message_id") in message_ids]
#         return []
#     except Exception as e:
#         print(f"❌ Fetch error: {e}")
#         return []


# # ========== INDEXING PIPELINE ==========

# def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
#     """Full pipeline: classify → embed → store in Data Store + Qdrant"""
    
#     # 1) Classify
#     category = classify_message(message_text)
#     print(f"📋 Classified as: {category}")
    
#     # 2) Store in Data Store
#     row_id = insert_into_datastore(
#         conversation_id, message_id, sender_id, timestamp_ms, message_text, category
#     )
    
#     # 3) Generate embedding
#     emb = embed_text(message_text)
#     dim = len(emb)
#     ensure_qdrant_collection(dim)
    
#     # 4) Index in Qdrant
#     point = PointStruct(
#         id=message_id,
#         vector=emb,
#         payload={
#             "conversation_id": conversation_id,
#             "sender_id": sender_id,
#             "category": category,
#             "row_id": row_id,  # Store Data Store ROWID for reference
#         },
#     )
#     qdrant.upsert(QDRANT_COLLECTION, [point])
#     print(f"✅ Indexed in Qdrant: {message_id}")


# # ========== SEARCH API ==========

# @app.route("/search", methods=["POST"])
# def search():
#     data = request.get_json(force=True)
#     query = data.get("query", "")
#     top_k = int(data.get("top_k", 5))
#     category = data.get("category")

#     if not query:
#         return jsonify({"error": "query required"}), 400

#     q_emb = embed_text(query)

#     q_filter = None
#     if category:
#         q_filter = Filter(
#             must=[FieldCondition(key="category", match=MatchValue(value=category))]
#         )

#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION,
#         query_vector=q_emb,
#         query_filter=q_filter,
#         limit=top_k,
#     )

#     ids = [str(h.id) for h in hits]
#     rows = fetch_by_message_ids(ids)

#     results = []
#     for h in hits:
#         row = next((r for r in rows if r.get("message_id") == str(h.id)), None)
#         if not row:
#             continue
#         results.append({
#             "message_id": h.id,
#             "score": h.score,
#             "message_text": row.get("message_text"),
#             "category": row.get("category"),
#             "conversation_id": row.get("conversation_id"),
#             "sender_id": row.get("sender_id"),
#             "time_stamp": row.get("time_stamp"),
#         })

#     return jsonify({"query": query, "results": results})
# # ========== OAUTH (unchanged) ==========

# @app.route('/login')
# def login():
#     params = {
#         'client_id': CLIENT_ID,
#         'scope': SCOPE,
#         'response_type': 'code',
#         'redirect_uri': REDIRECT_URI,
#         'access_type': 'offline',
#         'prompt': 'consent'
#     }
#     auth_request = requests.Request('GET', AUTH_URL, params=params).prepare()
#     return redirect(auth_request.url)


# @app.route('/oauth/callback')
# def oauth_callback():
#     global ACCESS_TOKEN, REFRESH_TOKEN
#     code = request.args.get('code')
#     if not code:
#         return "Missing authorization code", 400
    
#     data = {
#         'code': code,
#         'client_id': CLIENT_ID,
#         'client_secret': CLIENT_SECRET,
#         'redirect_uri': REDIRECT_URI,
#         'grant_type': 'authorization_code'
#     }
#     resp = requests.post(TOKEN_URL, data=data)
#     token_json = resp.json()
    
#     if 'access_token' not in token_json:
#         error = token_json.get('error', 'Unknown error')
#         error_description = token_json.get('error_description', '')
#         return f"Failed to get access token: {error} - {error_description}", 400
    
#     ACCESS_TOKEN = token_json.get('access_token')
#     REFRESH_TOKEN = token_json.get('refresh_token')
#     return "OAuth Successful!"


# # ========== PRODUCER: Deluge → Flask → Signals ==========

# @app.route('/bot/events', methods=['POST'])
# def bot_events():
#     form_data = request.form.to_dict()
#     print("---- Received from Deluge ----")
#     print(form_data)
    
#     operation = form_data.get('operation')
#     user_raw = form_data.get('user')
#     chat_raw = form_data.get('chat')
#     data_raw = form_data.get('data')
    
#     inner_data = {
#         "source": "zoho_cliq_bot",
#         "operation": operation,
#         "user": user_raw,
#         "chat": chat_raw,
#         "raw": data_raw
#     }
    
#     event_payload = {"data": inner_data}
    
#     if SIGNALS_EVENT_URL:
#         try:
#             resp = requests.post(
#                 SIGNALS_EVENT_URL,
#                 headers={"Content-Type": "application/json"},
#                 json=event_payload
#             )
#             print("Signals response:", resp.status_code, resp.text)
#         except Exception as e:
#             print("Error publishing to Signals:", e)
    
#     return jsonify({"status": "ok"})


# # ========== CONSUMER: Signals → Flask → Data Store + Qdrant ==========

# @app.route('/signals/consume', methods=['POST'])
# def signals_consume():
#     payload = request.get_json()
#     print("==== Signals delivered queued event ====")
#     print(json.dumps(payload, indent=2))

#     events = payload.get("events", [])
#     if not events:
#         return jsonify({"status": "no_events"}), 200

#     event_obj = events[0]
#     outer_data = event_obj.get("data", {})
#     inner_data = outer_data.get("data", {})

#     raw_str = inner_data.get("raw")
#     user_str = inner_data.get("user")
#     chat_str = inner_data.get("chat")

#     if not (raw_str and user_str and chat_str):
#         print("Missing raw/user/chat in inner_data; nothing to index")
#         return jsonify({"status": "ignored"}), 200

#     try:
#         raw = json.loads(raw_str)
#         user = json.loads(user_str)
#         chat = json.loads(chat_str)
#     except Exception as e:
#         print("Error parsing JSON fields:", e)
#         return jsonify({"status": "parse_error"}), 200

#     # Cliq structure: confirm with one log
#     print("Parsed raw:", json.dumps(raw, indent=2))
#     print("Parsed user:", json.dumps(user, indent=2))
#     print("Parsed chat:", json.dumps(chat, indent=2))

#     try:
#         message_text = raw["message"]["content"]["text"]
#         message_id = raw["message"]["id"]
#         timestamp_ms = raw["time"]
#     except KeyError as e:
#         print("Missing field in raw:", e)
#         return jsonify({"status": "bad_raw"}), 200

#     sender_id = user.get("zoho_user_id") or user.get("id")
#     conversation_id = chat.get("id")  # or channel_unique_name / channel_id

#     print(f"📨 {message_text!r}")
#     print(f"   conv={conversation_id}, sender={sender_id}, ts_ms={timestamp_ms}")

#     # Now run full indexing pipeline
#     index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)

#     return jsonify({"status": "processed"})
    



# if __name__ == '__main__':
#     app.run(port=8000, debug=True)
