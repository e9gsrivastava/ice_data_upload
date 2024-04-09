"""
serializers.py
"""
from rest_framework import serializers
from django.contrib.auth.models import User
from doc_upload.models import UploadedFile


class FileSerializer(serializers.ModelSerializer):
    """
    serializing data for User model
    """

    class Meta:
        model = UploadedFile
        fields = ["filename", "uploaded_at"]
