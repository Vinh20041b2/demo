<?php
$server = 'localhost';
$user = 'root';
$pass = '';
$database = 'bang';

// Kết nối đến MySQL
$conn = new mysqli($server, $user, $pass, $database);

// Kiểm tra kết nối
if ($conn->connect_error) {
    die("Kết nối thất bại: " . $conn->connect_error);
}

// Thiết lập kiểu chữ UTF-8 để hỗ trợ tiếng Việt
$conn->set_charset("utf8");

?>
