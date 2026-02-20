from flask import Flask, jsonify
from databricks import sql

app = Flask(__name__)

DATABRICKS_HOST = "dbc-c7eae79c-1039.cloud.databricks.com"
DATABRICKS_TOKEN = "dapidc03ff8e4dd701c54dfe58aa98c954d2"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/d8066d0900fe72a1"

@app.route("/api/data")
def get_data():
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM drag_sites.drag_chicago.drag_chicago_official LIMIT 10")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=True)