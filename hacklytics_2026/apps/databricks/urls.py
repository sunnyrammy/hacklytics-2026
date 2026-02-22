from django.urls import path

from .views import predict, products_collection, products_item

urlpatterns = [
    path("products/", products_collection, name="products_collection"),
    path("products/<str:product_name>/", products_item, name="products_item"),
    path("ml/predict", predict, name="ml-predict"),
]
