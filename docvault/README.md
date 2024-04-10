# docvault

This web application allows users to upload zip files containing PDF documents, extract them, and process the data within the PDFs. It is built using Django.

## Features

- Allows users to upload zip files containing PDF documents.
- Extracts the PDF documents from the uploaded zip file.
- Processes the data within the PDF documents.
- Users can specify a start date for processing the data.
- Provides feedback messages to users about the upload process.

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/e9gsrivastava/ice_data_upload.git
    ```

2. Navigate to the project directory:

    ```bash
    cd doc_vault
    ```

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

4. Run the Django server:

    ```bash
    python manage.py runserver
    ```

5. Access the application in your web browser at `http://localhost:8000`.

## Usage

1. Access the web application in your browser and log in to proceed to the upload page.
2. On the upload page, select a zip file containing PDF documents.
3. Specify a start date for processing the data by choosing a date from the calendar input.
4. Click the "Upload" button to initiate the upload and processing.
5. After the upload process completes, you will receive a message indicating the number of files uploaded and the username.
6. Additionally, the filenames are saved in the database (SQLite), and you can view them in the admin panel.
7. To view the list of uploaded filenames via API, you can send a GET request to `http://localhost:8000/api/v1/file`. Note that you must have the necessary permissions and be authenticated to access this endpoint.

## System Dependencies
## Note: Install JDK to run Tabula for PDF parsing
