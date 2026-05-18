require('dotenv').config({ override: true });
const express = require('express');
const crypto = require('crypto');
const { Kafka } = require('kafkajs');

const app = express();
// Đọc raw body dưới dạng chuỗi để tính toán chữ ký bảo mật chính xác
app.use(express.json({
    verify: (req, res, buf) => { req.rawBody = buf.toString(); }
}));

// Debug middleware - log tất cả requests
app.use((req, res, next) => {
    console.log(`[HTTP] ${req.method} ${req.path} - Headers:`, Object.keys(req.headers));
    next();
});

const PORT = process.env.PORT || 3001;
const VERIFY_TOKEN = process.env.FB_VERIFY_TOKEN;
const APP_SECRET = process.env.FB_APP_SECRET;
console.log('[DEBUG] Raw APP_SECRET:', JSON.stringify(APP_SECRET));

// Cấu hình kết nối tới mạng lưới Kafka Broker
const kafka = new Kafka({ clientId: 'webhook-service', brokers: [process.env.KAFKA_BROKER] });
const producer = kafka.producer();
let isKafkaConnected = false;

// 1. Endpoint GET /webhook: Dùng để xác minh liên kết ban đầu với Facebook Developer
app.get('/webhook', (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];

    if (mode && token) {
        if (mode === 'subscribe' && token === VERIFY_TOKEN) {
            console.log('[WEBHOOK] Xác minh liên kết với Meta thành công!');
            return res.status(200).send(challenge);
        }
        return res.status(403).send('Verify Token không chính xác');
    }
    res.status(400).send('Thiếu tham số cấu hình');
});

// 2. Endpoint POST /webhook: Nhận dữ liệu bình luận thời gian thực từ Facebook đổ về
app.post('/webhook', async (req, res) => {
    console.log('[WEBHOOK] ✓ POST request nhận được');
    
    // Nguyên tắc: Phải lập tức trả về 200 OK cho Facebook để tránh hệ thống gửi lặp lại
    res.status(200).send('EVENT_RECEIVED');

    // Kiểm tra và xác thực chữ ký bảo mật HMAC-SHA256
    const signature = req.headers['x-hub-signature-256'];
    console.log('[DEBUG] Signature header:', signature);
    if (!signature) return console.error('[CẢNH BÁO] Yêu cầu không chứa chữ ký bảo mật');

    const expectedSignature = 'sha256=' + crypto
        .createHmac('sha256', APP_SECRET)
        .update(req.rawBody || '')
        .digest('hex');

    console.log('[DEBUG] APP_SECRET length:', APP_SECRET ? APP_SECRET.length : 'undefined');
    console.log('[DEBUG] Received :', signature);
    console.log('[DEBUG] Expected  :', expectedSignature);
    if (signature !== expectedSignature) {
        console.error('[CẢNH BÁO] Chữ ký bảo mật sai lệch! Từ chối xử lý.');
        return;
    }
    console.log('[✓] Signature xác thực thành công!');

    try {
        const body = req.body;
        console.log('[DEBUG] Body received:', JSON.stringify(body, null, 2));
        if (body.object === 'page') {
            console.log('[✓] Body.object = page');
            for (const entry of body.entry) {
                if (!entry.changes) continue;
                for (const change of entry.changes) {
                    // Lọc đúng sự kiện khi có bình luận mới (comment add)
                    const pageId = entry.id;
                    const fromId = change.value.from?.id;
                    if (change.field === 'feed' && change.value.item === 'comment' && change.value.verb === 'add') {
                        if (fromId === pageId) {
                            console.log(`[SKIP] Comment từ chính Page (${fromId}), bỏ qua để tránh vòng lặp.`);
                            continue;
                        }
                        const value = change.value;
                        
                        // Chuẩn hóa (Normalize) về cấu trúc Schema nội bộ thống nhất
                        const normalizedEvent = {
                            schema_version: 1,
                            event_id: `evt_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
                            event_type: 'comment_created',
                            source: 'facebook',
                            page_id: entry.id,
                            post_id: value.post_id,
                            comment_id: value.comment_id,
                            user_id: value.from.id,
                            user_name: value.from.name,
                            message: value.message,
                            created_at: new Date(value.created_time * 1000).toISOString()
                        };

                        console.log(`[KAFKA] Đang publish sự kiện bình luận: "${normalizedEvent.message}" vào topic raw_events`);
                        
                        // Đẩy dữ liệu chuẩn hóa vào hàng đợi Kafka
                        if (isKafkaConnected) {
                            await producer.send({
                                topic: 'raw_events',
                                messages: [{ value: JSON.stringify(normalizedEvent) }]
                            });
                            console.log(`[✓] Event published to Kafka`);
                        } else {
                            console.warn(`[⚠] Kafka not connected, skipping publish`);
                        }
                    }
                }
            }
        }
    } catch (err) {
        console.error('[LỖI TRUYỀN DỮ LIỆU KAFKA]', err);
    }
});

async function start() {
    try {
        console.log('[INIT] Đang kết nối Kafka...');
        await producer.connect();
        isKafkaConnected = true;
        console.log('[✓] Kafka connected!');
    } catch (err) {
        isKafkaConnected = false;
        console.error('[✗] Kafka connection failed:', err.message);
        console.warn('[⚠] Tiếp tục chạy nhưng không có Kafka producer...');
    }
    
    app.listen(PORT, () => {
        console.log(` Webhook Service đang vận hành tại port ${PORT}`);
        console.log(`[✓] Ready to receive webhooks at http://localhost:${PORT}/webhook`);
    });
}
start();