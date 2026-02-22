"""
URL configuration for hacklytics_2026 project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path
from hacklytics_2026.apps.users.views import home
from hacklytics_2026.apps.databricks.views import live_audio_demo

urlpatterns = [
    path('admin/', admin.site.urls),
    path("databricks/", include("hacklytics_2026.apps.databricks.urls")),
    path("api/", include("hacklytics_2026.apps.databricks.urls")),
    path("demo/live-audio", live_audio_demo, name="live-audio-demo"),
    path("", home, name="home"),
]
