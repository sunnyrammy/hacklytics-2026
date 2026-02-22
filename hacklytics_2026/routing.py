from django.urls import path

from hacklytics_2026.apps.databricks.consumers import FlagAudioConsumer
from hacklytics_2026.apps.voicechats.consumers import VoiceChatStreamConsumer

websocket_urlpatterns = [
    path("ws/flag-audio/", FlagAudioConsumer.as_asgi()),
    path("ws/voicechat/stream/", VoiceChatStreamConsumer.as_asgi()),
]
