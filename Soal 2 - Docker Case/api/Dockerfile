# Gunakan `python:3.11.4` sebagai base image
# Menggunakan base image python versi 3.11.4
FROM python:3.11.4

# Copy file api.py serta requirements.txt ke docker container
COPY api.py api.py
COPY requirements.txt requirements.txt

# Menjalankan perintah pip install untuk menginstall package yang diperlukan di docker container
RUN pip install -r requirements.txt

# Menjalankan script api.py 
ENTRYPOINT [ "python", "api.py" ]