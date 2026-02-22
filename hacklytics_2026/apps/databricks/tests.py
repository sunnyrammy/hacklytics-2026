import json
import os
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from .databricks_client import DatabricksAPIError
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

    @patch.dict(os.environ, {"DATABRICKS_SERVING_ENDPOINT_NAME": "test-endpoint", "DATABRICKS_INPUT_COLUMN": "comment_text"}, clear=False)
    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_predict_with_text(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.query_serving_endpoint.return_value = {"result": [1]}

        response = self.client.post(
            "/api/ml/predict",
            data=json.dumps({"text": "hello"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("predictions", response.json())
        self.assertIn("elapsed_ms", response.json())
        mock_client.query_serving_endpoint.assert_called_once_with(
            "test-endpoint",
            {"dataframe_records": [{"comment_text": "hello"}]},
        )

    @patch.dict(os.environ, {"DATABRICKS_SERVING_ENDPOINT_NAME": "test-endpoint"}, clear=False)
    @patch("hacklytics_2026.apps.databricks.views.DatabricksClient")
    def test_predict_with_records(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.query_serving_endpoint.return_value = {"result": [0]}

        records = [{"sepal length (cm)": 5.1, "sepal width (cm)": 3.5}]
        response = self.client.post(
            "/api/ml/predict",
            data=json.dumps({"records": records}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        mock_client.query_serving_endpoint.assert_called_once_with(
            "test-endpoint",
            {"dataframe_records": records},
        )

    def test_predict_requires_text_or_records(self):
        response = self.client.post(
            "/api/ml/predict",
            data=json.dumps({"foo": "bar"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {"error": "Provide either 'records' or 'text' in the request body."},
        )

    @patch.dict(os.environ, {}, clear=True)
    def test_predict_missing_endpoint_env(self):
        response = self.client.post(
            "/api/ml/predict",
            data=json.dumps({"text": "hello"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json(),
            {"error": "DATABRICKS endpoint is not configured."},
        )


class DeployModelServingCommandTests(TestCase):
    @patch.dict(
        os.environ,
        {
            "DATABRICKS_HOST": "https://dbc-example.cloud.databricks.com",
            "DATABRICKS_TOKEN": "token",
            "DATABRICKS_SERVING_ENDPOINT_NAME": "test-endpoint",
            "DATABRICKS_MODEL_FULL_NAME": "catalog.schema.model",
            "DATABRICKS_MODEL_VERSION": "1",
        },
        clear=False,
    )
    @patch("hacklytics_2026.apps.databricks.management.commands.deploy_model_serving.DatabricksClient")
    def test_deploy_model_serving_existing_endpoint(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.create_serving_endpoint.side_effect = DatabricksAPIError("RESOURCE_ALREADY_EXISTS")
        mock_client.wait_endpoint_ready.return_value = {"state": {"ready": "READY"}}
        stdout = StringIO()

        call_command("deploy_model_serving", stdout=stdout)

        mock_client.create_serving_endpoint.assert_called_once()
        mock_client.wait_endpoint_ready.assert_called_once_with(endpoint_name="test-endpoint")
        self.assertIn("already exists", stdout.getvalue())
        self.assertIn("READY", stdout.getvalue())

    @patch.dict(
        os.environ,
        {
            "DATABRICKS_HOST": "https://dbc-example.cloud.databricks.com",
            "DATABRICKS_TOKEN": "token",
            "DATABRICKS_SERVING_ENDPOINT_NAME": "test-endpoint",
            "DATABRICKS_MODEL_FULL_NAME": "catalog.schema.model",
            "DATABRICKS_MODEL_VERSION": "1",
        },
        clear=False,
    )
    @patch("hacklytics_2026.apps.databricks.management.commands.deploy_model_serving.DatabricksClient")
    def test_deploy_model_serving_invalid_config_value_error(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.create_serving_endpoint.side_effect = ValueError("Databricks host is not configured.")

        with self.assertRaises(CommandError) as exc:
            call_command("deploy_model_serving")

        self.assertIn("Invalid Databricks serving configuration", str(exc.exception))
