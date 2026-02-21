import json
from unittest.mock import patch

from django.test import TestCase

from . import services


class DatabricksViewsTests(TestCase):
    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_get_products_json(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.query_all.return_value = [
            {"product_name": "Widget", "price": 10},
            {"product_name": "Gadget", "price": 20},
        ]

        response = self.client.get("/databricks/products/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "products": [
                    {"product_name": "Widget", "price": 10},
                    {"product_name": "Gadget", "price": 20},
                ]
            },
        )
        mock_client.query_all.assert_called_once_with(services.LIST_PRODUCTS_SQL)

    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_get_products_html(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.query_all.return_value = [{"product_name": "Widget", "price": 10}]

        response = self.client.get("/databricks/products/?format=html")

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "databricks/products_list.html")
        self.assertContains(response, "Widget")
        self.assertContains(response, "10")

    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_create_product_calls_insert(self, mock_client_cls):
        mock_client = mock_client_cls.return_value

        payload = {"product_name": "Widget", "price": 25}
        response = self.client.post(
            "/databricks/products/",
            data=json.dumps(payload),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        mock_client.execute.assert_called_once_with(services.INSERT_PRODUCT_SQL, ("Widget", 25))

    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_update_product_calls_update(self, mock_client_cls):
        mock_client = mock_client_cls.return_value

        response = self.client.put(
            "/databricks/products/Widget/",
            data=json.dumps({"price": 99}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        mock_client.execute.assert_called_once_with(services.UPDATE_PRODUCT_SQL, (99, "Widget"))

    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_delete_product_calls_delete(self, mock_client_cls):
        mock_client = mock_client_cls.return_value

        response = self.client.delete("/databricks/products/Widget/")

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        mock_client.execute.assert_called_once_with(services.DELETE_PRODUCT_SQL, ("Widget",))

    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_create_product_validation_error(self, mock_client_cls):
        response = self.client.post(
            "/databricks/products/",
            data=json.dumps({"product_name": "Widget", "price": "not-int"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.json())
        mock_client_cls.assert_not_called()

    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_update_product_validation_error(self, mock_client_cls):
        response = self.client.put(
            "/databricks/products/Widget/",
            data=json.dumps({"price": "not-int"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.json())
        mock_client_cls.assert_called_once()

    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient", side_effect=ValueError("Databricks configuration is incomplete."))
    def test_missing_config_returns_500(self, _mock_client_cls):
        response = self.client.get("/databricks/products/")

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json(),
            {"error": "Databricks integration is not configured."},
        )
