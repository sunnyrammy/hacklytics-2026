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
export DATABRICKS_SERVER_HOSTNAME="<your-server-hostname>"
export DATABRICKS_HTTP_PATH="<your-http-path>"
export DATABRICKS_TOKEN="<your-personal-access-token>"
```

Never commit tokens or secret values to git.

## Run migrations

```bash
python manage.py migrate
```

## Start server

```bash
python manage.py runserver
```

## Homepage

Open:
- http://127.0.0.1:8000/

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

## Databricks credential locations

- Server Hostname + HTTP Path:
  - Databricks SQL Warehouse -> connection details
- Token:
  - User Settings -> Developer -> Personal Access Tokens

## Security notes

- Do not hardcode Databricks credentials in code.
- Do not commit `.env` or token values.
