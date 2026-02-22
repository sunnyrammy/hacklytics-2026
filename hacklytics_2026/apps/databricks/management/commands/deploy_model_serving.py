import os

from django.core.management.base import BaseCommand, CommandError

from hacklytics_2026.apps.databricks.databricks_client import (
    DatabricksAPIError,
    DatabricksClient,
)


class Command(BaseCommand):
    help = "Create Databricks Model Serving endpoint and wait until READY."

    def handle(self, *args, **options):
        endpoint_name = os.getenv("DATABRICKS_SERVING_ENDPOINT_NAME")
        model_full_name = os.getenv("DATABRICKS_MODEL_FULL_NAME")
        model_version = os.getenv("DATABRICKS_MODEL_VERSION")

        if not endpoint_name or not model_full_name or not model_version:
            raise CommandError(
                "Missing required environment variables: "
                "DATABRICKS_SERVING_ENDPOINT_NAME, DATABRICKS_MODEL_FULL_NAME, DATABRICKS_MODEL_VERSION."
            )

        served_entity_name = os.getenv("DATABRICKS_SERVED_ENTITY_NAME") or f"{endpoint_name}-entity"
        workload_size = os.getenv("DATABRICKS_WORKLOAD_SIZE", "Small")
        scale_to_zero = os.getenv("DATABRICKS_SCALE_TO_ZERO", "true").strip().lower() == "true"

        try:
            client = DatabricksClient()
        except ValueError as exc:
            raise CommandError(
                "Databricks integration is not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN."
            ) from exc

        try:
            client.create_serving_endpoint(
                endpoint_name=endpoint_name,
                model_full_name=model_full_name,
                model_version=model_version,
                workload_size=workload_size,
                scale_to_zero=scale_to_zero,
                served_entity_name=served_entity_name,
            )
            self.stdout.write(self.style.WARNING(f"Created endpoint '{endpoint_name}'."))
        except DatabricksAPIError as exc:
            message = str(exc)
            if "RESOURCE_ALREADY_EXISTS" in message or "already exists" in message.lower():
                self.stdout.write(self.style.WARNING(f"Endpoint '{endpoint_name}' already exists; checking status."))
            else:
                raise CommandError(f"Failed to create endpoint '{endpoint_name}': {message}") from exc

        try:
            state = client.wait_endpoint_ready(endpoint_name=endpoint_name)
        except TimeoutError as exc:
            raise CommandError(str(exc)) from exc
        except DatabricksAPIError as exc:
            raise CommandError(f"Failed while waiting for endpoint readiness: {exc}") from exc

        ready_state = (state.get("state") or {}).get("ready", "UNKNOWN")
        self.stdout.write(self.style.SUCCESS(f"Endpoint '{endpoint_name}' is READY ({ready_state})."))
