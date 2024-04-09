# urls.py
from django.views.generic import TemplateView
from django.urls import path
from .views import FileUploadView
from .views import CustomLoginView, CustomLogoutView


app_name = "doc_upload"


urlpatterns = [
    path("upload/", FileUploadView.as_view(), name="file_upload"),
    path(
        "upload/success/",
        TemplateView.as_view(template_name="upload_success.html"),
        name="upload_success",
    ),
    path("", CustomLoginView.as_view(), name="login"),
    path("logout/", CustomLogoutView.as_view(), name="logout"),
]
