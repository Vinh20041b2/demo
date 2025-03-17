import paho.mqtt.client as mqtt
import mysql.connector
import json
import time
import datetime

# Cấu hình MQTT Broker
MQTT_BROKER = "e8fa3a03f84a4ba6ae0a38cb4c21a9e6.s1.eu.hivemq.cloud"
MQTT_PORT = 8883  # SSL/TLS
MQTT_USERNAME = "vuvuvu"
MQTT_PASSWORD = "1234567890Aa"
MQTT_TOPIC_TRACUU = "web/tracuu"
MQTT_TOPIC_KETQUA = "web/ketqua"

# Cấu hình MySQL
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "vu"

# Kết nối MySQL
def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Lỗi kết nối MySQL: {e}")
        return None

# Lấy dữ liệu theo yêu cầu từ bảng sensor
def get_filtered_data(name, start_date, end_date):
    conn = connect_mysql()
    if conn is None:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT * FROM `sensor`
            WHERE `NAME` = %s AND `TIME` BETWEEN %s AND %s
            ORDER BY `TIME` ASC
        """
        cursor.execute(query, (name, start_date, end_date))
        results = cursor.fetchall()

        # Chuyển đổi TIME từ datetime sang chuỗi
        for row in results:
            if isinstance(row["TIME"], (datetime.date, datetime.datetime)):
                row["TIME"] = row["TIME"].strftime("%Y-%m-%d %H:%M:%S")

        return results
    except Exception as e:
        print(f"❌ Lỗi truy vấn MySQL: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# Hàm xử lý khi nhận tin nhắn từ topic `web/tracuu`
def on_message(client, userdata, msg):
    try:
        request_data = json.loads(msg.payload.decode("utf-8"))
        print(f"📥 Nhận yêu cầu tra cứu: {request_data}")

        name = request_data.get("type")
        start_date = request_data.get("startDate")
        end_date = request_data.get("endDate")

        if not name or not start_date or not end_date:
            print("⚠️ Yêu cầu không hợp lệ!")
            return

        results = get_filtered_data(name, start_date, end_date)

        if results:
            for row in results:
                json_data = json.dumps(row, ensure_ascii=False)
                client.publish(MQTT_TOPIC_KETQUA, json_data)
                print(f"🚀 Đã gửi kết quả: {json_data}")
        else:
            print("⚠️ Không tìm thấy dữ liệu phù hợp!")
    except json.JSONDecodeError as e:
        print(f"❌ Lỗi xử lý JSON: {e}")

# Kết nối MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.tls_set()
client.on_message = on_message

try:
    print("🔗 Đang kết nối MQTT...")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC_TRACUU)
    print(f"📡 Đã kết nối MQTT - Lắng nghe topic: {MQTT_TOPIC_TRACUU}")

    client.loop_forever()
except KeyboardInterrupt:
    print("🛑 Dừng chương trình...")
    client.loop_stop()
except Exception as e:
    print(f"❌ Lỗi kết nối MQTT: {e}")
    client.loop_stop()
