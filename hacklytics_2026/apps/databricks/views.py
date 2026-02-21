import json

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from .databricks_client import DatabricksClient
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
