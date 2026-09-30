FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml requirements.txt ./
COPY eventvault ./eventvault
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir --no-deps . \
    && useradd --create-home --uid 10001 eventvault
USER eventvault
EXPOSE 8000
CMD ["eventvault", "run", "--host", "0.0.0.0"]
