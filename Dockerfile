FROM mageai/mageai:latest

# Add custom Python packages
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt
