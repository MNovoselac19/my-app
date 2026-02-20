from flask import Flask, jsonify
from databricks import sql
import time

app = Flask(__name__)

DATABRICKS_HOST = "dbc-c7eae79c-1039.cloud.databricks.com"
DATABRICKS_TOKEN = "dapidc03ff8e4dd701c54dfe58aa98c954d2"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/d8066d0900fe72a1"

cache = {"data": None, "timestamp": 0}
CACHE_DURATION = 600  # 10 minutes

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET'
    return response

@app.route("/api/data")
def get_data():
    now = time.time()
    if cache["data"] and (now - cache["timestamp"]) < CACHE_DURATION:
        return jsonify(cache["data"])

    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM drag_sites.drag_chicago.drag_chicago_official")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]

    cache["data"] = result
    cache["timestamp"] = now
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=True)