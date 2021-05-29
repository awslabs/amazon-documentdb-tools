FROM python:3

COPY . .
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT [ "python", "migrationtools/documentdb_index_tool.py" ]
CMD ["--help"]
