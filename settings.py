import os

email = os.environ.get('login_email')
password = os.environ.get('senha_email')

EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = 'mx.balaroti.com.br' 
EMAIL_USE_SSL = True
EMAIL_PORT = 465
EMAIL_HOST_USER = email
EMAIL_HOST_PASSWORD = password