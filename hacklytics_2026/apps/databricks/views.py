import json
import os
import time

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .databricks_client import DatabricksAPIError, DatabricksClient
from .services import create_product, delete_product, list_products, update_product_price


GENERIC_ERROR_MESSAGE = "Unable to process Databricks request."
CONFIG_ERROR_MESSAGE = "Databricks integration is not configured."


def _get_client() -> DatabricksClient:
    return DatabricksClient()


def _parse_json_body(request: HttpRequest) -> dict:
    try:
        body = request.body.decode("utf-8") if request.body else "{}"
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("Invalid JSON payload.")

    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")
    return payload


def _json_error(message: str, status: int) -> JsonResponse:
    return JsonResponse({"error": message}, status=status)


@require_http_methods(["GET", "POST"])
def products_collection(request: HttpRequest) -> HttpResponse:
    if request.method == "GET":
        try:
            client = _get_client()
            products = list_products(client)
        except ValueError:
            return _json_error(CONFIG_ERROR_MESSAGE, 500)
        except Exception:
            return _json_error(GENERIC_ERROR_MESSAGE, 500)

        wants_html = request.GET.get("format") == "html" or "text/html" in request.headers.get(
            "Accept", ""
        )
        if wants_html:
            return render(request, "databricks/products_list.html", {"products": products})
        return JsonResponse({"products": products}, status=200)

    try:
        payload = _parse_json_body(request)
        product_name = payload.get("product_name")
        price = payload.get("price")

        if not isinstance(product_name, str) or not product_name.strip():
            raise ValueError("product_name must be a non-empty string.")
        if not isinstance(price, int):
            raise ValueError("price must be an integer.")

        client = _get_client()
        create_product(client, product_name.strip(), price)
        return JsonResponse(
            {"message": "Product created.", "product_name": product_name.strip(), "price": price},
            status=201,
        )
    except ValueError as exc:
        if str(exc) == "Databricks configuration is incomplete.":
            return _json_error(CONFIG_ERROR_MESSAGE, 500)
        return _json_error(str(exc), 400)
    except Exception:
        return _json_error(GENERIC_ERROR_MESSAGE, 500)


@require_http_methods(["PUT", "DELETE"])
def products_item(request: HttpRequest, product_name: str) -> HttpResponse:
    sanitized_name = product_name.strip()
    if not sanitized_name:
        return _json_error("product_name must be provided.", 400)

    try:
        client = _get_client()

        if request.method == "PUT":
            payload = _parse_json_body(request)
            price = payload.get("price")
            if not isinstance(price, int):
                raise ValueError("price must be an integer.")

            update_product_price(client, sanitized_name, price)
            return JsonResponse(
                {"message": "Product updated.", "product_name": sanitized_name, "price": price},
                status=200,
            )

        delete_product(client, sanitized_name)
        return HttpResponse(status=204)
    except ValueError as exc:
        if str(exc) == "Databricks configuration is incomplete.":
            return _json_error(CONFIG_ERROR_MESSAGE, 500)
        return _json_error(str(exc), 400)
    except Exception:
        return _json_error(GENERIC_ERROR_MESSAGE, 500)


@csrf_exempt
@require_http_methods(["POST"])
def predict(request: HttpRequest) -> HttpResponse:
    try:
        payload = _parse_json_body(request)
    except ValueError as exc:
        return _json_error(str(exc), 400)

    records = payload.get("records")
    if records is not None:
        if not isinstance(records, list) or not all(isinstance(item, dict) for item in records):
            return _json_error("'records' must be a list of JSON objects.", 400)
        invocation_payload = {"dataframe_records": records}
    elif "text" in payload:
        text = payload.get("text")
        if not isinstance(text, str) or not text.strip():
            return _json_error("'text' must be a non-empty string.", 400)
        input_column = os.getenv("DATABRICKS_INPUT_COLUMN", "comment_text")
        invocation_payload = {"dataframe_records": [{input_column: text}]}
    else:
        return _json_error("Provide either 'records' or 'text' in the request body.", 400)

    endpoint_name = os.getenv("DATABRICKS_SERVING_ENDPOINT_NAME")
    if not endpoint_name:
        return _json_error("DATABRICKS_SERVING_ENDPOINT_NAME is not configured.", 500)

    try:
        client = _get_client()
        start = time.perf_counter()
        response = client.query_serving_endpoint(endpoint_name, invocation_payload)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return JsonResponse({"predictions": response, "elapsed_ms": elapsed_ms}, status=200)
    except ValueError:
        return _json_error(CONFIG_ERROR_MESSAGE, 500)
    except DatabricksAPIError as exc:
        return _json_error(f"Inference request failed: {exc}", 502)
    except Exception:
        return _json_error(GENERIC_ERROR_MESSAGE, 500)


@require_http_methods(["GET"])
def live_audio_demo(request: HttpRequest) -> HttpResponse:
    return render(request, "databricks/live_audio_demo.html")
