# Hacklytics 2026

Django application for Hacklytics 2026 with:
- Product CRUD APIs backed by Databricks SQL
- Databricks model-serving inference proxy
- Real-time voice transcription (Vosk) over HTTP and WebSockets
- Local lexicon-based content flagging for transcript segments

## Tech Stack

- Python + Django
- ASGI with Daphne and Channels
- Redis (for Channels WebSocket layer)
- Databricks SQL Connector
- Vosk speech-to-text

## Project Structure

- `hacklytics_2026/apps/users/` - homepage and user app
- `hacklytics_2026/apps/databricks/` - product APIs, Databricks client, model inference API
- `hacklytics_2026/apps/voicechats/` - voicechat UI, transcription APIs, flagging logic

## Quick Start

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Apply migrations:

```bash
python manage.py migrate
```

4. Start the ASGI server:

```bash
daphne -b 0.0.0.0 -p 8000 hacklytics_2026.asgi:application
```

5. Open the app:

- Home: http://127.0.0.1:8000/
- Voicechat: http://127.0.0.1:8000/voicechat/
- Live audio demo: http://127.0.0.1:8000/demo/live-audio

## Environment Variables

### Required for Databricks Features

```bash
export DATABRICKS_HOST="https://dbc-xxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-personal-access-token>"
```

Set one of the following endpoint variables for inference:

```bash
export DATABRICKS_ENDPOINT="my-serving-endpoint"
# or
export DATABRICKS_SERVING_ENDPOINT_NAME="my-serving-endpoint"
```

### Optional Databricks Variables

```bash
export DATABRICKS_SERVER_HOSTNAME="<warehouse-hostname>"
export DATABRICKS_HTTP_PATH="<warehouse-http-path>"
export DATABRICKS_INPUT_COLUMN="text"
export TOXICITY_THRESHOLD="0.7"
```

### Voicechat / Local Flagging

```bash
export VOSK_MODEL_PATH="/absolute/or/project-relative/path/to/vosk-model"
export FLAGGING_PROVIDER="lexicon"
export FLAG_TERMS_PATH="hacklytics_2026/apps/voicechats/flagging/sample_flag_terms.json"
```

Notes:
- A sample lexicon file with placeholders is committed for safe local testing.
- Keep private lexicons and credentials outside git.

## Redis and WebSockets

WebSockets require ASGI (`daphne`) and Redis when using `channels_redis`.

Start Redis with Docker:

```bash
docker run --rm -p 6379:6379 redis:7
```

WebSocket endpoints:
- `ws://127.0.0.1:8000/ws/flag-audio/`
- `ws://127.0.0.1:8000/ws/voicechat/stream/`

Use `wss://` in production.

## API Endpoints

### Databricks Product CRUD

- `GET /databricks/products/`
- `POST /databricks/products/`
- `PUT /databricks/products/<product_name>/`
- `DELETE /databricks/products/<product_name>/`

Examples:

```bash
# List products (JSON)
curl -X GET http://127.0.0.1:8000/databricks/products/

# Create product
curl -X POST http://127.0.0.1:8000/databricks/products/ \
  -H "Content-Type: application/json" \
  -d '{"product_name":"Widget","price":100}'

# Update product price
curl -X PUT http://127.0.0.1:8000/databricks/products/Widget/ \
  -H "Content-Type: application/json" \
  -d '{"price":120}'

# Delete product
curl -X DELETE http://127.0.0.1:8000/databricks/products/Widget/
```

### Model Serving Inference

- `POST /api/ml/predict`

Examples:

```bash
# Text payload
curl -X POST http://127.0.0.1:8000/api/ml/predict \
  -H "Content-Type: application/json" \
  -d '{"text":"hello"}'

# Record payload
curl -X POST http://127.0.0.1:8000/api/ml/predict \
  -H "Content-Type: application/json" \
  -d '{"records":[{"sepal length (cm)":5.1,"sepal width (cm)":3.5,"petal length (cm)":1.4,"petal width (cm)":0.2}]}'
```

### Voicechat Health + Transcription APIs

- `GET /api/voicechat/health/`
- `POST /api/voicechat/transcribe/`
- `POST /api/voicechat/finalize/`

Health endpoint should report:
- `vosk_model_loaded: true` when model path is valid
- `flag_terms_loaded: true` when lexicon file is valid

## Management Commands

Deploy model serving resources:

```bash
python manage.py deploy_model_serving
```

## Security

- Never commit tokens, secrets, or private lexicons.
- Prefer environment variables for all credentials.
- Do not commit `.env` files containing secrets.
