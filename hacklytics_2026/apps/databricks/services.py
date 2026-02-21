from .databricks_client import DatabricksClient

LIST_PRODUCTS_SQL = "SELECT product_name, price FROM products ORDER BY product_name"
INSERT_PRODUCT_SQL = "INSERT INTO products (product_name, price) VALUES (?, ?)"
UPDATE_PRODUCT_SQL = "UPDATE products SET price = ? WHERE product_name = ?"
DELETE_PRODUCT_SQL = "DELETE FROM products WHERE product_name = ?"


def list_products(client: DatabricksClient) -> list[dict]:
    return client.query_all(LIST_PRODUCTS_SQL)


def create_product(client: DatabricksClient, product_name: str, price: int) -> None:
    client.execute(INSERT_PRODUCT_SQL, (product_name, price))


def update_product_price(client: DatabricksClient, product_name: str, price: int) -> None:
    client.execute(UPDATE_PRODUCT_SQL, (price, product_name))


def delete_product(client: DatabricksClient, product_name: str) -> None:
    client.execute(DELETE_PRODUCT_SQL, (product_name,))
