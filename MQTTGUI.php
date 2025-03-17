<?php
require("myweb/phpMQTT.php"); 
//require __DIR__ . "/phpMQTT.php";
//require_once __DIR__ . "/phpMQTT.php";

$server = "2e6e9401b7da4d2dac5ff9585f005417.s1.eu.hivemq.cloud";
$port = 8883;
$username = "VINH1";
$password = "Vinh1234";
$client_id = "php-mqtt-client-" . uniqid(); 

$topic = "mySQL/gui"; 

// Kết nối MySQL
$db = new mysqli("localhost", "root", "", "bang");
if ($db->connect_error) {
    die("Lỗi kết nối MySQL: " . $db->connect_error);
}

// Kết nối MQTT
$mqtt = new phpMQTT($server, $port, $client_id);

if (!$mqtt->connect(true, NULL, $username, $password)) {
    die("Không thể kết nối MQTT");
}

// Hàm xử lý tin nhắn nhận được từ MQTT
$mqtt->subscribe([$topic => ["qos" => 0, "function" => "procMsg"]]);

function procMsg($topic, $msg) {
    global $db;
    echo "Nhận dữ liệu từ MQTT: $msg\n";
    
    // Chuyển JSON thành mảng PHP
    $data = json_decode($msg, true);
    
    if (isset($data['id']) && isset($data['tên']) && isset($data['dữ liệu']) && isset($data['kết quả'])) {
        $id = $db->real_escape_string($data['id']);
        $ten = $db->real_escape_string($data['tên']);
        $dulieu = $db->real_escape_string($data['dữ liệu']);
        $ketqua = $db->real_escape_string($data['kết quả']);

        // Chèn vào bảng MySQL
        $sql = "INSERT INTO sensor_data (id, ten, dulieu, ketqua) VALUES ('$id', '$ten', '$dulieu', '$ketqua')";
        if ($db->query($sql) === TRUE) {
            echo "Dữ liệu đã lưu thành công vào MySQL\n";
        } else {
            echo "Lỗi MySQL: " . $db->error;
        }
    } else {
        echo "Dữ liệu JSON không hợp lệ\n";
    }
}

// Chạy vòng lặp MQTT
while ($mqtt->proc()) {}

$mqtt->close();
$db->close();
?>
