FROM python:3

COPY . .
RUN pip install --no-cache-dir -r index-tool/requirements.txt
ENTRYPOINT [ "python", "index-tool/migrationtools/documentdb_index_tool.py" ]
CMD ["--help"]
