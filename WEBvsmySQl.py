import paho.mqtt.client as mqtt
import mysql.connector
import json
import datetime

# Cấu hình MQTT Broker
MQTT_BROKER = "e8fa3a03f84a4ba6ae0a38cb4c21a9e6.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
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

# Chuyển đổi từ "HH:mm dd/MM/yyyy" (JSON) -> "YYYY-MM-DD HH:MM:SS" (MySQL)
def convert_json_to_mysql(time_str):
    try:
        dt_obj = datetime.datetime.strptime(time_str, "%H:%M %d/%m/%Y")
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print(f"❌ Lỗi định dạng thời gian từ JSON: {e}")
        return None

# Truy vấn dữ liệu từ MySQL theo điều kiện
def get_filtered_data(sensor_type, start_datetime, end_datetime):
    conn = connect_mysql()
    if conn is None:
        return []

    try:
        cursor = conn.cursor(dictionary=True)
        
        # Nếu type là "all", lấy tất cả dữ liệu trong khoảng thời gian
        if sensor_type == "all":
            query = """
                SELECT * FROM `sensor`
                WHERE `TIME` BETWEEN %s AND %s
                ORDER BY `TIME` ASC
            """
            cursor.execute(query, (start_datetime, end_datetime))
        else:
            query = """
                SELECT * FROM `sensor`
                WHERE `NAME` = %s AND `TIME` BETWEEN %s AND %s
                ORDER BY `TIME` ASC
            """
            cursor.execute(query, (sensor_type, start_datetime, end_datetime))

        results = cursor.fetchall()

        # 🔹 Chuyển đổi TIME từ datetime -> string để tránh lỗi JSON
        for row in results:
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

        sensor_type = request_data.get("type")
        start_datetime_str = request_data.get("startDateTime")
        end_datetime_str = request_data.get("endDateTime")

        if not sensor_type or not start_datetime_str or not end_datetime_str:
            print("⚠️ Yêu cầu không hợp lệ!")
            return

        # Chuyển đổi thời gian từ JSON sang MySQL
        start_datetime = convert_json_to_mysql(start_datetime_str)
        end_datetime = convert_json_to_mysql(end_datetime_str)

        if not start_datetime or not end_datetime:
            print("⚠️ Lỗi định dạng thời gian, không thể xử lý yêu cầu!")
            return

        # Kiểm tra nếu start_datetime > end_datetime
        if start_datetime > end_datetime:
            print("⚠️ Thời gian bắt đầu không thể lớn hơn thời gian kết thúc!")
            return

        results = get_filtered_data(sensor_type, start_datetime, end_datetime)

        if results:
            for row in results:
                json_data = json.dumps(row, ensure_ascii=False)
                client.publish(MQTT_TOPIC_KETQUA, json_data)
                print(f"🚀 Đã gửi dữ liệu: {json_data}")
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
