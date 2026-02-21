# hacklytics_2026

## Databricks Delta Integration (Django + Databricks SQL Connector)

This project keeps SQLite as the default Django database and uses the Databricks SQL Connector to run SQL directly against Databricks Delta tables.

## 1) Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Configure environment variables

```bash
export DATABRICKS_SERVER_HOSTNAME="<your-server-hostname>"
export DATABRICKS_HTTP_PATH="<your-http-path>"
export DATABRICKS_TOKEN="<your-personal-access-token>"
```

Never commit tokens or secret values to git.

## 3) Run migrations

```bash
python manage.py migrate
```

## 4) Run server

```bash
python manage.py runserver
```

## 5) Sync Databricks products into SQLite cache

```bash
python manage.py sync_products_from_delta
```

## 6) CRUD API examples

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

## 7) Databricks UI locations for credentials

- `DATABRICKS_SERVER_HOSTNAME` and `DATABRICKS_HTTP_PATH`:
  - Go to your Databricks SQL Warehouse and open connection details.
- `DATABRICKS_TOKEN`:
  - Go to User Settings -> Developer -> Personal Access Tokens and generate a token.

## Security notes

- Do not hardcode Databricks credentials in code.
- Do not commit `.env` or token values.
