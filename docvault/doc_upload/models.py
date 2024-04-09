"""
model class
"""

from django.db import models
from django.contrib.auth import get_user_model


class UploadedFile(models.Model):
    """
    model class for to sotre fields-filename and upload time
    """

    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    filename = models.CharField(max_length=255)
    uploaded_at = models.DateTimeField(auto_now_add=True)
