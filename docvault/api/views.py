"""
views.py
"""
from rest_framework import generics
from doc_upload.models import UploadedFile
from .serializers import FileSerializer


class FileUpload(generics.ListAPIView):
    """
    API view for retrieving, updating, and deleting a Book object.
    """

    queryset = UploadedFile.objects.all()
    serializer_class = FileSerializer

