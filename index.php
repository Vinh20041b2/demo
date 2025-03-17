<?php
	include 'connect.php';
	<?php
	// Kiểm tra tệp connect.php
	if (file_exists('connect.php')) {
		include 'connect.php';
		echo "Tệp connect.php được bao gồm thành công<br>";
	} else {
		die("Tệp connect.php không tồn tại<br>");
	}

	// Kiểm tra tệp phpMQTT.php
	if (file_exists('phpMQTT.php')) {
		require("phpMQTT.php");
		echo "Tệp phpMQTT.php được bao gồm thành công<br>";
	} else {
		die("Tệp phpMQTT.php không tồn tại<br>");
	}

	// Nếu không có lỗi, tiếp tục thực thi mã
	echo "XAMPP hoạt động bình thường<br>";<?php
	// Kiểm tra tệp connect.php
	if (file_exists('connect.php')) {
		include 'connect.php';
		echo "Tệp connect.php được bao gồm thành công<br>";
	} else {
		die("Tệp connect.php không tồn tại<br>");
	}

	// Kiểm tra tệp phpMQTT.php
	if (file_exists('phpMQTT.php')) {
		require("phpMQTT.php");
		echo "Tệp phpMQTT.php được bao gồm thành công<br>";
	} else {
		die("Tệp phpMQTT.php không tồn tại<br>");
	}

	// Nếu không có lỗi, tiếp tục thực thi mã
	echo "XAMPP hoạt động bình thường<br>";
?>

