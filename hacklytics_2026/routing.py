from django.urls import path

from hacklytics_2026.apps.databricks.consumers import FlagAudioConsumer

websocket_urlpatterns = [
    path("ws/flag-audio/", FlagAudioConsumer.as_asgi()),
]
