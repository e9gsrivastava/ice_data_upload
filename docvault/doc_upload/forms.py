"""
forms.py
"""
from django import forms
from .models import UploadedFile


class UploadFileForm(forms.Form):
    """
    form class to create a form for uploading file
    """

    file = forms.FileField()

    def __init__(self, *args, **kwargs):
        super(UploadFileForm, self).__init__(*args, **kwargs)
        self.fields["file"].widget.attrs.update({"class": "form-control-file"})
