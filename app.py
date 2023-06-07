from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Summary, Gauge, Histogram, Counter
import psycopg2
import re
from flask import Flask, Response
from waitress import serve

app = Flask(__name__)


_INF = float("inf")


postgres_con = psycopg2.connect(
    host="176.9.226.3",
    database="alon_123cluster",
    user="alon",
    password="dJuYAw9urvpCX9PCCBDmwzsQDWY"
)


gauge_dict = {}

query = '''SELECT 
    subq."name",
    subq.monitor_name,
    subq.db_type,
    subq.id,
    subq.severity as severity_color,
    COALESCE(ra.severity, FLOOR(RANDOM() * 3) + 1) AS last_severity
FROM (
    SELECT 
        CASE 
            WHEN m.db_type LIKE 'ORACLE%' THEN n."name" || '@' || (SELECT "name" FROM "clusterCore_c123_host" WHERE id = n.host_id) || ':' || n.port
            ELSE n.name            
        END AS "name",
        m.monitor_name,
        m.db_type,
        n.id,
        n.severity,
        MAX(a.alert_id) AS max_alert_id
    FROM 
        app_123monitor.rp_monitors m
        JOIN "clusterCore_c123_node" n ON m.db_type = n."type" AND m.db_type != 'LINUX'
        LEFT JOIN app_123monitor.rp_alert a ON m.monitor_name = a.monitor_name AND n.id = a.node_id AND m.db_type = a.node_type
    WHERE 
        m.org_id = 1
        AND n.ind_deleted = 'false'
    GROUP BY 
        "name",
        m.monitor_name,
        m.db_type,
        n.id,
        n.severity
    UNION 
    SELECT 
        h.name AS "name",
        m.monitor_name,
        m.db_type,
        h.id,
        n.severity,
        MAX(a.alert_id) AS max_alert_id
    FROM 
        app_123monitor.rp_monitors m
        JOIN "clusterCore_c123_node" n ON m.db_type = 'LINUX'
        JOIN "clusterCore_c123_host" h ON m.db_type = 'LINUX'
        LEFT JOIN app_123monitor.rp_alert a ON m.monitor_name = a.monitor_name AND h.id = a.node_id AND m.db_type = a.node_type
    WHERE 
        m.org_id = 1
        AND h.ind_deleted = 'false'
        AND m.db_type = 'LINUX'
    GROUP BY 
        h.name,
        m.monitor_name,
        m.db_type,
        n.severity,
        h.id
) AS subq
LEFT JOIN app_123monitor.rp_alert ra ON subq.max_alert_id = ra.alert_id
ORDER BY subq.id, subq.monitor_name, subq."name"; '''


def replace_characters_with_underscore(text):
   
    pattern = r'[^a-zA-Z0-9_]'

    modified_text = re.sub(pattern, '_', text)

    return modified_text



@app.route('/metrics')
def process_metrics():
    cursor = postgres_con.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()


    for row in rows:
        node_name = row[0]
        monitor_name = replace_characters_with_underscore(row[1])
        node_type = row[2]
        severity = row[5]
        severity_color = row[4]

        if monitor_name in gauge_dict:
           gauge = gauge_dict[monitor_name]
        else:
           gauge = Gauge(f"my_dynamic_metric_{monitor_name}", f"A dynamic metric for {monitor_name}", ['node_type', 'node_name', 'severity_color'])
           gauge_dict[monitor_name] = gauge
           gauge.labels(node_type, node_name, severity_color).set(severity)

    metrics_data = generate_latest()
    cursor.close()
    return Response(metrics_data, mimetype=CONTENT_TYPE_LATEST)


if __name__ == '__main__':
    serve(app, host="0.0.0.0", port=8080)



    # for row in rows:
    #     node_name = row[2].replace(" ", "_").replace("/", "_").replace(".", "_").replace("&", "_").replace("(", "_").replace(")", "_")
    #     node_type = row[3]
    #     severity = row[4]

    #     if node_name in gauge_dict:
    #        gauge = gauge_dict[node_name]
    #     else:
    #        gauge = Gauge(f"my_dynamic_metric_{node_name}", f"A dynamic metric for {node_name}", ['status'])
    #        gauge_dict[node_name] = gauge
    #        gauge.labels(node_type).set(severity)



        # for row in rows:
        # node_name = row[0]
        # status = row[1]
        # cpu_usage = row[2]
        
        # if node_name in gauge_dict:
        #     gauge = gauge_dict[node_name]
        # else:
        #     gauge = Gauge(f"my_dynamic_metric_{node_name}", f"A dynamic metric for {node_name}", ['status'])
        #     gauge_dict[node_name] = gauge
        #     gauge.labels(status).set(cpu_usage)