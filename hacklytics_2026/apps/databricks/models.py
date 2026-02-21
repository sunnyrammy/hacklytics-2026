from django.db import models


class ProductCache(models.Model):
    product_name = models.CharField(max_length=200, unique=True)
    price = models.IntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"{self.product_name} ({self.price})"
