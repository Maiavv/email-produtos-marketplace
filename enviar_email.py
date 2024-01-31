import os
import django
import datetime

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE", "settings"
)  
django.setup()

from django.core.mail import send_mail

recipient_list = ["ly.salles@balaroti.com.br"]

def enviar_email(subject, message, recipient_list):
    send_mail(
        subject,
        message = 'O que exatamente você está fazendo com minha imagem?',
        "",
        recipient_list,
        fail_silently=False,
    )


