from django.urls import path

from .views import products_collection, products_item

urlpatterns = [
    path("products/", products_collection, name="products_collection"),
    path("products/<str:product_name>/", products_item, name="products_item"),
]
