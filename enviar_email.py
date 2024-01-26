import os
import django
import datetime

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "settings"
)  # Substitua 'seu_projeto.settings' pelo caminho correto
django.setup()

from django.core.mail import send_mail


def enviar_email(subject, message, recipient_list):
    send_mail(
        subject,
        message,
        "vitor.maia@balaroti.com.br",
        recipient_list,
        fail_silently=False,
    )


