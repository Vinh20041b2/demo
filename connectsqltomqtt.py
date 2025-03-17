import paho.mqtt.client as mqtt
import mysql.connector
import json

# Cấu hình MQTT Broker (HiveMQ Cloud)
MQTT_BROKER = "2e6e9401b7da4d2dac5ff9585f005417.s1.eu.hivemq.cloud"
MQTT_PORT = 8883  # SSL/TLS
MQTT_USERNAME = "VINH1"
MQTT_PASSWORD = "Vinh1234"
MQTT_TOPIC = "SQL/nhan"

# Cấu hình MySQL
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "bang"

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
    except mysql.connector.Error as err:
        print(f"❌ Lỗi kết nối MySQL: {err}")
        return None

# Xử lý khi nhận tin nhắn MQTT
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        print(f"📥 Nhận dữ liệu từ MQTT: {data}")

        # Kiểm tra dữ liệu hợp lệ
        if all(key in data for key in ["id", "name", "data", "KQ"]):
            conn = connect_mysql()
            if conn:
                try:
                    cursor = conn.cursor()
                    sql = "INSERT INTO                     pip install paho-mqttvinh.vu (id, name, data, KQ) VALUES (%s, %s, %s, %s)"
                    values = (data["id"], data["name"], data["data"], data["KQ"])
                    cursor.execute(sql, values)
                    print(values)
                    conn.commit()
                    print("✅ Dữ liệu đã lưu vào MySQL")
                except mysql.connector.Error as err:
                    print(f"❌ Lỗi MySQL: {err}")
                finally:
                    cursor.close()
                    conn.close()
        else:
            print("⚠️ JSON không hợp lệ! Thiếu trường dữ liệu cần thiết.")
    
    except json.JSONDecodeError as e:
        print(f"❌ Lỗi đọc JSON: {e}")

# Kết nối MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.tls_set()  # Kết nối SSL/TLS
client.on_message = on_message

try:
    print("🔗 Đang kết nối MQTT...")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    print(f"📡 Đã kết nối MQTT - Đang lắng nghe topic: {MQTT_TOPIC}")
    
    client.loop_forever()  # Chạy vòng lặp nhận dữ liệu MQTT

except Exception as e:
    print(f"❌ Lỗi kết nối MQTT: {e}")  
