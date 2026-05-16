require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { Kafka } = require('kafkajs');
const { Client } = require('pg');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const DB_URL = "postgresql://fb_api_user:fb_api_password@localhost:5432/fb_api_db";

// Cấu hình Kết nối Database và Kafka
const db = new Client({ connectionString: DB_URL });
const kafka = new Kafka({ clientId: 'backend-api', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'backend-api-group' });
const producer = kafka.producer();

// CẤU HÌNH CIRCUIT BREAKER (Mẫu thiết kế ngắt mạch - Mục 5.3)
const circuitBreaker = {
    state: 'CLOSED', // Có 3 trạng thái: CLOSED, OPEN, HALF-OPEN
    failureCount: 0,
    failureThreshold: 5,     // Lỗi liên tiếp 5 lần sẽ mở mạch
    cooldownPeriod: 30000,   // Thời gian ngắt mạch thử lại: 30 giây
    lastStateChange: Date.now()
};

// Hàm giả lập gọi API Facebook Graph (Ẩn comment hoặc Gửi tin nhắn reply)
async function callFacebookGraphAPI(command) {
    // Nếu trạng thái đang là OPEN, kiểm tra xem đã qua thời gian chờ chưa để chuyển sang HALF-OPEN
    if (circuitBreaker.state === 'OPEN') {
        if (Date.now() - circuitBreaker.lastStateChange > circuitBreaker.cooldownPeriod) {
            circuitBreaker.state = 'HALF-OPEN';
            console.log('⚡ [CIRCUIT BREAKER] Chuyển sang trạng thái HALF-OPEN. Thử nghiệm gửi lại...');
        } else {
            throw new Error('CIRCUIT_BREAKER_OPEN: Hệ thống tạm ngắt kết nối gọi sang Facebook.');
        }
    }

    try {
        console.log(`[FACEBOOK API] Đang thực thi hành động: ${command.action} cho comment ${command.comment_id}`);
        
        // GIẢ LẬP LỖI MẠNG: Để test kịch bản lỗi, nếu tin nhắn chứa từ "error" -> Giả lập Facebook sập
        if (command.reply_text && command.reply_text.toLowerCase().includes('error')) {
            throw new Error('Facebook API Timeout (500 Internal Error)');
        }

        // --- ĐOẠN CODE GỌI API THẬT (Nếu có Token thật) ---
        // await axios.post(`https://graph.facebook.com/v26.0/${command.comment_id}/replies`, { message: command.reply_text, access_token: '...' });
        
        // Nếu gọi thành công, reset bộ đếm lỗi của Circuit Breaker
        if (circuitBreaker.state === 'HALF-OPEN') {
            circuitBreaker.state = 'CLOSED';
            circuitBreaker.failureCount = 0;
            console.log('🟢 [CIRCUIT BREAKER] Dịch vụ đã hồi phục. Đóng mạch (CLOSED).');
        }
        return true;
    } catch (err) {
        // Xử lý đếm lỗi cho Circuit Breaker
        circuitBreaker.failureCount++;
        console.error(`❌ [LỖI GỌI FB] Lần thất bại liên tiếp thứ: ${circuitBreaker.failureCount}`);
        
        if (circuitBreaker.failureCount >= circuitBreaker.failureThreshold) {
            circuitBreaker.state = 'OPEN';
            circuitBreaker.lastStateChange = Date.now();
            console.error('🔴 [CIRCUIT BREAKER] Lỗi liên tiếp quá ngưỡng! Mở mạch (OPEN). Tạm ngừng gọi Facebook trong 30s.');
        }
        throw err;
    }
}

// HÀM TIÊU THỤ VÀ XỬ LÝ LỆNH TỪ KAFKA
async function processCommand(command, topicName) {
    const { command_id, event_id, reply_text, action } = command;

    // 1. KIỂM TRA TÍNH IDEMPOTENT (Chống xử lý trùng lặp - Mục 5.3)
    const checkKey = await db.query('SELECT * FROM idempotency_keys WHERE command_id = $1', [command_id]);
    if (checkKey.rows.length > 0) {
        console.log(`⚠️ [IDEMPOTENCY] Bỏ qua! Command_id ${command_id} đã được xử lý trước đó rồi.`);
        return; // Thoát ngay lập tức, không gửi trùng tin nhắn lên Facebook
    }

    try {
        // 2. Tiến hành gọi sang Facebook API thông qua bộ ngắt mạch Circuit Breaker
        await callFacebookGraphAPI(command);

        // 3. Nếu thành công -> Lưu khóa Idempotency key lại vào Database với trạng thái 'SUCCESS'
        await db.query(
            "INSERT INTO idempotency_keys (command_id, status) VALUES ($1, $2)",
            [command_id, 'SUCCESS']
        );
        console.log(`✅ [SUCCESS] Xử lý thành công câu lệnh ${command_id}. Đã khóa Idempotency Key.`);

    } catch (error) {
        console.error(`[THẤT BẠI] Gửi Facebook thất bại. Đang chuyển lỗi sang Retry Service...`);

        // 4. Nếu thất bại -> Đóng gói payload và bắn vào topic 'send_failed' để Retry Service xử lý
        const failedPayload = {
            schema_version: 1,
            command_id,
            event_id,
            retry_count: command.retry_count || 0, // Nếu từ topic reply_commands sang thì mặc định là lần 0
            last_error: error.message,
            next_retry_at: new Date().toISOString(),
            payload: { action, reply_text, comment_id: command.comment_id, page_id: command.page_id }
        };

        await producer.send({
            topic: 'send_failed',
            messages: [{ value: JSON.stringify(failedPayload) }]
        });
    }
}

async function start() {
    await db.connect();
    await consumer.connect();
    await producer.connect();

    // Lắng nghe từ cả 2 topic: Lệnh mới từ Core Service và Lệnh thử lại từ Retry Service
    await consumer.subscribe({ topics: ['reply_commands', 'send_retry'], fromBeginning: false });
    console.log(' Backend API đang lắng nghe hàng đợi reply_commands và send_retry...');

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const command = JSON.parse(message.value.toString());
            await processCommand(command, topic);
        },
    });

    app.listen(PORT, () => console.log(`Server Dashboard REST API đang chạy tại cổng ${PORT}`));
}

start().catch(console.error);