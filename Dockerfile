FROM python:3

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD [ "python", "migrationtools/documentdb_index_tool.py" ]