from django.core.management.base import BaseCommand, CommandError

from hacklytics_2026.apps.databricks.databricks_client import DatabricksClient
from hacklytics_2026.apps.databricks.models import ProductCache
from hacklytics_2026.apps.databricks.services import list_products


class Command(BaseCommand):
    help = "Sync products from Databricks Delta table into SQLite ProductCache"

    def handle(self, *args, **options):
        try:
            client = DatabricksClient()
            products = list_products(client)
        except ValueError as exc:
            raise CommandError("Databricks integration is not configured.") from exc
        except Exception as exc:
            raise CommandError("Failed to fetch products from Databricks.") from exc

        created_count = 0
        updated_count = 0

        for product in products:
            product_name = product.get("product_name")
            price = product.get("price")

            if not isinstance(product_name, str) or not product_name.strip() or not isinstance(price, int):
                continue

            _, created = ProductCache.objects.update_or_create(
                product_name=product_name.strip(),
                defaults={"price": price},
            )
            if created:
                created_count += 1
            else:
                updated_count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Synced {len(products)} products (created={created_count}, updated={updated_count})."
            )
        )
