"""
ASGI config for hacklytics_2026 project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/6.0/howto/deployment/asgi/
"""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hacklytics_2026.settings')

django_asgi_app = get_asgi_application()

try:
    from channels.auth import AuthMiddlewareStack
    from channels.routing import ProtocolTypeRouter, URLRouter

    from hacklytics_2026.routing import websocket_urlpatterns  # noqa: E402

    application = ProtocolTypeRouter(
        {
            "http": django_asgi_app,
            "websocket": AuthMiddlewareStack(URLRouter(websocket_urlpatterns)),
        }
    )
except ImportError:
    # Fallback for environments where Channels is not installed yet.
    application = django_asgi_app
