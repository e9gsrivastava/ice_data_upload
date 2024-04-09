"""
views.py
"""
import io
import zipfile
from django.urls import reverse, reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.views import LoginView, LogoutView
from django.views.generic import FormView
from doc_upload.local import extract_and_upload_ice_data
from .forms import UploadFileForm


def get_files_in_zip(zip_file_path):
    """
    to get the number of files in the zip folder
    """
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        file_list = zip_ref.namelist()
        return len(file_list)


class FileUploadView(LoginRequiredMixin, FormView):
    """
    code to upload the file
    """

    template_name = "upload.html"
    form_class = UploadFileForm
    login_url = "/login/"

    def get_success_url(self):
        return reverse("doc_upload:file_upload")

    def form_valid(self, form):
        uploaded_file = form.cleaned_data["file"]
        user = self.request.user
        file_contents = uploaded_file.read()

        if zipfile.is_zipfile(io.BytesIO(file_contents)):
            # Get the temporary path of the uploaded file
            uploaded_file_path = uploaded_file.temporary_file_path()

            target_directory = "/home/fox/developer/automate_upload/data/temp"
            with zipfile.ZipFile(uploaded_file_path, "r") as zip_ref:
                zip_ref.extractall(target_directory)

            num_files = get_files_in_zip(uploaded_file_path)

            # Call final function after file extraction
            extract_and_upload_ice_data(uploaded_file_path)
            message = f"{num_files} files have been uploaded by {user}."

            return self.render_to_response(
                self.get_context_data(form=form, message=message)
            )

        message = "Uploaded file is not a zip file."
        return self.render_to_response(
            self.get_context_data(form=form, message=message)
        )


class CustomLoginView(LoginView):
    """
    this is Login view
    """

    template_name = "login.html"
    next_page = "/upload"


class CustomLogoutView(LogoutView):
    """
    this is logout view
    """

    next_page = reverse_lazy("doc_upload:login")
