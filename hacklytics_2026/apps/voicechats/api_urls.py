from django.urls import path

from . import views

urlpatterns = [
    path("health/", views.health, name="voicechat-health"),
    path("transcribe/", views.transcribe_chunk, name="voicechat-transcribe"),
    path("finalize/", views.finalize_stream, name="voicechat-finalize"),
]
