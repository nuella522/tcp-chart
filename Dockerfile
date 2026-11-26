FROM python:3.12-slim
WORKDIR /app
RUN mkdir -p /data
COPY server.py .
COPY auth_store.py .
COPY message_store.py .
ENV CHAT_HOST=0.0.0.0
ENV CHAT_PORT=5000
ENV WEB_PORT=8080
ENV CHAT_DB_PATH=/data/chat.sqlite3
EXPOSE 5000 8080
CMD ["python", "-u", "server.py"]