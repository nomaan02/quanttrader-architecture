FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY services ./services
COPY scripts ./scripts
# Run as a non-root user
RUN useradd --create-home app
USER app
