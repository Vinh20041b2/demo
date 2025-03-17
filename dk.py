import json
import mysql.connector
import datetime
import time
import paho.mqtt.client as mqtt

# Cấu hình MQTT
MQTT_BROKER = "e8fa3a03f84a4ba6ae0a38cb4c21a9e6.s1.eu.hivemq.cloud"
MQTT_PORT = 8883  # SSL/TLS
MQTT_USERNAME = "vuvuvu"
MQTT_PASSWORD = "1234567890Aa"
MQTT_TOPIC_NHAN = "esp/nhan"
MQTT_TOPIC_GUI = "esp/dk"

# Cấu hình MySQL
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "vu"

# Hàm kết nối MySQL
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

# Hàm lưu dữ liệu vào MySQL
def save_to_database(data):
    conn = connect_mysql()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()
        query = """
            INSERT INTO `dk` (`den`, `toc_do_quat`, `may_bom`, `may_suoi`, `time`)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            data["den"],
            data["toc_do_quat"],
            data["may_bom"],
            data["may_suoi"],
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        cursor.execute(query, values)
        conn.commit()
        print(f"✅ Dữ liệu đã được lưu vào MySQL: {values}")
        return True
    except Exception as e:
        print(f"❌ Lỗi lưu MySQL: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# Hàm lấy dữ liệu mới nhất từ MySQL và gửi lên MQTT
def send_latest_data():
    conn = connect_mysql()
    if conn is None:
        return

    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM dk ORDER BY time DESC LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            result["time"] = result["time"].strftime("%Y-%m-%d %H:%M:%S")  # Chuyển datetime thành chuỗi
            mqtt_payload = json.dumps(result)
            client.publish(MQTT_TOPIC_GUI, mqtt_payload)
            print(f"📤 Đã gửi dữ liệu mới nhất lên MQTT topic esp/dkdk: {mqtt_payload}")
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu mới nhất: {e}")
    finally:
        cursor.close()
        conn.close()

# Hàm xử lý khi nhận dữ liệu từ ESP qua MQTT
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        print(f"📥 Nhận dữ liệu từ ESP: {data}")

        # Danh sách key cần có
        required_keys = ["control_den", "control_quat", "control_maybom", "control_maysuoi"]

        # Kiểm tra nếu thiếu key
        if not all(key in data for key in required_keys):
            print("⚠️ Dữ liệu không hợp lệ, thiếu trường!")
            return

        # Chuyển đổi dữ liệu từ string -> int
        formatted_data = {
            "den": int(data["control_den"]),
            "toc_do_quat": int(data["control_quat"]),
            "may_bom": int(data["control_maybom"]),
            "may_suoi": int(data["control_maysuoi"]),
        }

        # Lưu vào MySQL
        if save_to_database(formatted_data):
            print("🚀 Dữ liệu đã được ghi vào MySQL!")
            send_latest_data()  # Gửi dữ liệu mới nhất lên MQTT
        else:
            print("⚠️ Không thể lưu dữ liệu vào MySQL!")

    except json.JSONDecodeError as e:
        print(f"❌ Lỗi xử lý JSON: {e}")
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")

# Kết nối MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.tls_set()
client.on_message = on_message

try:
    print("🔗 Đang kết nối MQTT...")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC_NHAN)
    print(f"📡 Đã kết nối MQTT - Lắng nghe topic: {MQTT_TOPIC_NHAN}")

    client.loop_forever()
except KeyboardInterrupt:
    print("🛑 Dừng chương trình...")
    client.loop_stop()
except Exception as e:
    print(f"❌ Lỗi kết nối MQTT: {e}")
    client.loop_stop()
