# hacklytics_2026

Minimal Django app for Hacklytics 2026 with:
- A Tailwind (CDN) template homepage at `/`
- Databricks Delta access via Databricks SQL Connector endpoints at `/databricks/products/`

## Run with conda

```bash
conda create -n hacklytics_2026 python=3.14 -y
conda activate hacklytics_2026
pip install -r requirements.txt
```

## Configure Databricks environment variables (for Databricks endpoints)

```bash
export DATABRICKS_HOST="https://dbc-xxxx.cloud.databricks.com"
export DATABRICKS_SERVER_HOSTNAME="<your-server-hostname>"
export DATABRICKS_HTTP_PATH="<your-http-path>"
export DATABRICKS_TOKEN="<your-personal-access-token>"

# Model Serving deployment/inference
export DATABRICKS_SERVING_ENDPOINT_NAME="my-serving-endpoint"
export DATABRICKS_MODEL_FULL_NAME="workspace.default.elasticnet_iris"
export DATABRICKS_MODEL_VERSION="1"

# Optional
export DATABRICKS_SERVED_ENTITY_NAME="my-serving-endpoint-entity"
export DATABRICKS_WORKLOAD_SIZE="Small"
export DATABRICKS_SCALE_TO_ZERO="true"
export DATABRICKS_INPUT_COLUMN="comment_text"
export TOXICITY_THRESHOLD="0.7"
export SCORE_EVERY_SECONDS="1.0"
export VOSK_MODEL_PATH="models/vosk-model-small-en-us-0.15"
export DATABRICKS_ENDPOINT="my-serving-endpoint" # optional alias for serving endpoint
```

Never commit tokens or secret values to git.

## Run migrations

```bash
python manage.py migrate
```

## Start server

```bash
daphne -b 0.0.0.0 -p 8000 hacklytics_2026.asgi:application
```

## Homepage

Open:
- http://127.0.0.1:8000/

## WebSocket live audio setup (Redis + Channels)

Start Redis (Docker):

```bash
docker run --rm -p 6379:6379 redis:7
```

Or use a local Redis server:

```bash
redis-server
```

Then run ASGI via Daphne:

```bash
daphne -b 0.0.0.0 -p 8000 hacklytics_2026.asgi:application
```

Open demo page:
- http://127.0.0.1:8000/demo/live-audio

WebSocket endpoint:
- ws://127.0.0.1:8000/ws/flag-audio/ (use `wss://` in production)
- ws://127.0.0.1:8000/ws/voicechat/stream/ (voicechat live transcription)

Note: WebSockets require ASGI (`daphne`); `manage.py runserver` is not required.

## Voicechat live transcription

1. Install dependencies:
```bash
pip install -r requirements.txt
```
2. Place a Vosk model folder locally and set:
```bash
export VOSK_MODEL_PATH="/absolute/or/project-relative/path/to/vosk-model"
```
3. Configure Databricks variables:
```bash
export DATABRICKS_HOST="https://dbc-xxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="<token>"
export DATABRICKS_ENDPOINT="<endpoint-name-or-/serving-endpoints/.../invocations>"
# Output normalization contract (backend-only, frontend always receives 0.0-1.0 score)
export DATABRICKS_SCORE_TYPE="probability_0_1"  # probability_0_1 | percent_0_100 | logit | none
export DATABRICKS_SCORE_FIELD="score"           # optional field path, e.g. predictions.0.score
export DATABRICKS_LABEL_FIELD="label"           # optional field path
export DATABRICKS_POSITIVE_CLASS="flag"         # optional label used for flagging
# Optional per-endpoint override map (JSON string):
# export DATABRICKS_ENDPOINT_OUTPUT_SPECS='{"endpoint-a":{"score_type":"percent_0_100","score_field":"toxicity"}}'
```
4. Run ASGI server:
```bash
daphne -b 0.0.0.0 -p 8000 hacklytics_2026.asgi:application
```
5. Open:
```text
http://127.0.0.1:8000/voicechat/
```

### Quick verification checklist

- `GET /api/voicechat/health/` returns `"vosk_model_loaded": true` when `VOSK_MODEL_PATH` is valid.
- `GET /api/voicechat/health/` returns `"databricks_reachable": true` when Databricks host/token/endpoint are valid.
- Speaking into the mic shows live partial transcript updates in `/voicechat/`.
- Finalized transcript segments emit Databricks score results (or `score_error` details if unreachable).

## Databricks CRUD examples

List products (JSON):

```bash
curl -X GET http://127.0.0.1:8000/databricks/products/
```

List products (HTML):

```bash
curl -H "Accept: text/html" http://127.0.0.1:8000/databricks/products/
```

Create product:

```bash
curl -X POST http://127.0.0.1:8000/databricks/products/ \
  -H "Content-Type: application/json" \
  -d '{"product_name":"Widget","price":100}'
```

Update product price:

```bash
curl -X PUT http://127.0.0.1:8000/databricks/products/Widget/ \
  -H "Content-Type: application/json" \
  -d '{"price":120}'
```

Delete product:

```bash
curl -X DELETE http://127.0.0.1:8000/databricks/products/Widget/
```

## Model Serving deployment

```bash
python manage.py deploy_model_serving
```

## Model Serving inference proxy

Text payload:

```bash
curl -X POST http://localhost:8000/api/ml/predict \
  -H "Content-Type: application/json" \
  -d '{"text":"hello"}'
```

Records payload:

```bash
curl -X POST http://localhost:8000/api/ml/predict \
  -H "Content-Type: application/json" \
  -d '{"records":[{"sepal length (cm)":5.1,"sepal width (cm)":3.5,"petal length (cm)":1.4,"petal width (cm)":0.2}]}'
```

## Databricks credential locations

- Server Hostname + HTTP Path:
  - Databricks SQL Warehouse -> connection details
- Token:
  - User Settings -> Developer -> Personal Access Tokens

## Security notes

- Do not hardcode Databricks credentials in code.
- Do not commit `.env` or token values.
