import paho.mqtt.client as mqtt
import mysql.connector
import json
# 🚀 Cấu hình MQTT Broker (HiveMQ Cloud)
MQTT_BROKER = "e8fa3a03f84a4ba6ae0a38cb4c21a9e6.s1.eu.hivemq.cloud"
MQTT_PORT = 8883  # SSL/TLS
MQTT_USERNAME = "vuvuvu"
MQTT_PASSWORD = "1234567890Aa"
MQTT_TOPIC = "esp/cambien"

# 🚀 Cấu hình MySQL
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "bang"
TABLE_NAME = "cacmdm_bien"

# 🔗 Kết nối MySQL
def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Lỗi kết nối MySQL: {e}")
        return None

# 🔨 Tạo database & bảng nếu chưa có
def setup_database():
    conn = connect_mysql()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
        conn.database = MYSQL_DATABASE  # Chọn database

        # Tạo bảng nếu chưa tồn tại
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            data FLOAT NOT NULL,
            KQ TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Đã kiểm tra & tạo bảng thành công!")
        return True
    except mysql.connector.Error as e:
        print(f"❌ Lỗi khi tạo database/bảng: {e}")
        return False

# 💾 Chèn dữ liệu vào MySQL
def insert_data(sensor_id, name, data, KQ):
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor()
        sql = f"""
        INSERT INTO {TABLE_NAME} (id, name, data, KQ) 
        VALUES (%s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE name=%s, data=%s, KQ=%s
        """
        values = (sensor_id, name, data, KQ, name, data, KQ)
        cursor.execute(sql, values)
        conn.commit()
        print(f"✅ Đã lưu vào MySQL: ID={sensor_id}, Name={name}, Data={data}, KQ={KQ}")
    except mysql.connector.Error as e:
        print(f"❌ Lỗi chèn dữ liệu: {e}")
    finally:
        cursor.close()
        conn.close()

# 📥 Xử lý khi nhận dữ liệu từ MQTT
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        print(f"📥 Nhận từ MQTT: {data}")

        # Kiểm tra dữ liệu hợp lệ
        if "id" in data and "name" in data and "data" in data and "KQ" in data:
            insert_data(data["id"], data["name"], data["data"], data["KQ"])
        else:
            print("⚠️ Dữ liệu nhận không hợp lệ!")
    except Exception as e:
        print(f"❌ Lỗi xử lý MQTT: {e}")

# 🔗 Kết nối MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.tls_set()
client.on_message = on_message

try:
    print("🔗 Đang kết nối MQTT...")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    print(f"📡 Đã kết nối MQTT - Đang lắng nghe topic: {MQTT_TOPIC}")

    setup_database()  # Tạo database & bảng nếu chưa có

    client.loop_forever()  # Lắng nghe MQTT liên tục
except Exception as e:
    print(f"❌ Lỗi kết nối MQTT: {e}")
