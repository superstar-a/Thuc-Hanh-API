require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const { Kafka } = require('kafkajs');

const app = express();
// Đọc raw body dưới dạng chuỗi để tính toán chữ ký bảo mật chính xác
app.use(express.json({
    verify: (req, res, buf) => { req.rawBody = buf.toString(); }
}));

const PORT = process.env.PORT || 3001;
const VERIFY_TOKEN = process.env.FB_VERIFY_TOKEN;
const APP_SECRET = process.env.FB_APP_SECRET;

// Cấu hình kết nối tới mạng lưới Kafka Broker
const kafka = new Kafka({ clientId: 'webhook-service', brokers: [process.env.KAFKA_BROKER] });
const producer = kafka.producer();

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
    // Nguyên tắc: Phải lập tức trả về 200 OK cho Facebook để tránh hệ thống gửi lặp lại
    res.status(200).send('EVENT_RECEIVED');

    // Kiểm tra và xác thực chữ ký bảo mật HMAC-SHA256
    const signature = req.headers['x-hub-signature-256'];
    if (!signature) return console.error('[CẢNH BÁO] Yêu cầu không chứa chữ ký bảo mật');

    const expectedSignature = 'sha256=' + crypto
        .createHmac('sha256', APP_SECRET)
        .update(req.rawBody || '')
        .digest('hex');

    if (signature !== expectedSignature) {
        return console.error('[CẢNH BÁO] Chữ ký bảo mật sai lệch! Từ chối xử lý.');
    }

    try {
        const body = req.body;
        if (body.object === 'page') {
            for (const entry of body.entry) {
                if (!entry.changes) continue;
                for (const change of entry.changes) {
                    // Lọc đúng sự kiện khi có bình luận mới (comment add)
                    if (change.field === 'feed' && change.value.item === 'comment' && change.value.verb === 'add') {
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
                            message: value.message,
                            created_at: new Date(value.created_time * 1000).toISOString()
                        };

                        console.log(`[KAFKA] Đang publish sự kiện bình luận: "${normalizedEvent.message}" vào topic raw_events`);
                        
                        // Đẩy dữ liệu chuẩn hóa vào hàng đợi Kafka
                        await producer.send({
                            topic: 'raw_events',
                            messages: [{ value: JSON.stringify(normalizedEvent) }]
                        });
                    }
                }
            }
        }
    } catch (err) {
        console.error('[LỖI TRUYỀN DỮ LIỆU KAFKA]', err);
    }
});

async function start() {
    await producer.connect();
    app.listen(PORT, () => console.log(` Webhook Service đang vận hành tại port ${PORT}`));
}
start();