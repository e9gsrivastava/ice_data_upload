"""
views.py
"""
import io
import zipfile
from django.urls import reverse, reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.views import LoginView, LogoutView
from django.views.generic import FormView
from doc_upload.upload_ice_data_temp import extract_and_upload_ice_data
from .forms import UploadFileForm
from .models import UploadedFile

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
            # Get the temporary path of the uploaded folder
            uploaded_file_path = uploaded_file.temporary_file_path()

            target_directory = "/home/fox/developer/automate_upload/data/temp"
            with zipfile.ZipFile(uploaded_file_path, "r") as zip_ref:
                zip_ref.extractall(target_directory)

            # Get start date from dashboard
            start_date = self.request.POST.get("start_date")
            print(start_date,'444444444444444444444444444444444444444444444444444444444444444444444')

            # Call final function after file extraction
            num_files = extract_and_upload_ice_data(start_date,uploaded_file_path)
            message = f"{num_files} files have been uploaded by {user}."

            # Save file to db
            with zipfile.ZipFile(uploaded_file_path, "r") as zip_ref:
                file_names = zip_ref.namelist()
                for file_name in file_names:
                    UploadedFile.objects.create(
                        filename=file_name,
                        user=user
                    )

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
