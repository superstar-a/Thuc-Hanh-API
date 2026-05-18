require('dotenv').config();
const { Kafka } = require('kafkajs');
const { Client } = require('pg');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const geminiModel = genAI.getGenerativeModel({ model: 'gemini-2.5-flash' }, { apiVersion: 'v1' });

// Khởi tạo kết nối PostgreSQL Database
const db = new Client({ connectionString: process.env.DB_URL });

// Khởi tạo kết nối Kafka Broker
const kafka = new Kafka({ clientId: 'core-service', brokers: [process.env.KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: 'core-service-group' });
const producer = kafka.producer();

// 1. Hàm phát hiện Spam cơ bản (Yêu cầu mục 4.2)
function checkSpamLogic(message) {
    // Luật 1: Kiểm tra xem bình luận có chứa liên kết (link) hay không
    const hasLink = /(https?:\/\/[^\s]+)/g.test(message);
    if (hasLink) return 'spam_link';
    
    return 'clean';
}

// 2. Hàm gọi Gemini AI để nhận diện Ý định (Intent) và Cảm xúc (Sentiment)
async function analyzeWithAI(message) {
    const prompt = `Phân tích bình luận khách hàng sau và trả về JSON với 2 trường:
- intent: một trong ['ask_price', 'complaint', 'compliment', 'interact']
- sentiment: một trong ['positive', 'negative', 'neutral']

Bình luận: "${message}"

Chỉ trả về JSON thuần, không giải thích. Ví dụ: {"intent":"complaint","sentiment":"negative"}`;

    try {
        const result = await geminiModel.generateContent(prompt);
        const text = result.response.text().trim();
        const jsonMatch = text.match(/\{[^}]+\}/);
        if (jsonMatch) {
            const parsed = JSON.parse(jsonMatch[0]);
            return {
                intent: parsed.intent || 'interact',
                sentiment: parsed.sentiment || 'neutral'
            };
        }
    } catch (err) {
        console.error('[GEMINI ERROR]', err.message);
        console.log('[AI] Dùng keyword fallback...');
        return keywordAnalyze(message);
    }

    return keywordAnalyze(message);
}

function keywordAnalyze(message) {
    const msg = message.toLowerCase();
    const positiveWords = ['thích', 'tốt', 'tuyệt', 'nhanh', 'hay', 'oke', 'ok', 'ngon', 'chất', 'đỉnh', 'cảm ơn', 'cám ơn', 'hài lòng', 'ủng hộ', 'recommend', 'yêu'];
    const negativeWords = ['thất vọng', 'tệ', 'chán', 'tức', 'bực', 'lâu', 'chờ', 'hỏng', 'lỗi', 'vỡ', 'giả', 'fake', 'không được', 'không tốt', 'kém'];
    const priceWords = ['giá', 'bao nhiêu', 'tiền', 'phí', 'cost', 'price', 'báo giá', 'mua', 'order'];

    if (positiveWords.some(w => msg.includes(w))) return { intent: 'compliment', sentiment: 'positive' };
    if (negativeWords.some(w => msg.includes(w))) return { intent: 'complaint', sentiment: 'negative' };
    if (priceWords.some(w => msg.includes(w))) return { intent: 'ask_price', sentiment: 'neutral' };
    return { intent: 'interact', sentiment: 'neutral' };
}

// 3. Hàm xử lý luồng dữ liệu chính của từng sự kiện
async function handleEvent(event) {
    console.log(`\n[CONSUME] Nhận comment từ Kafka: "${event.message}"`);

    try {
        // Bước A: Lưu thông tin gốc vào database với trạng thái 'received'
        await db.query(
            `INSERT INTO comments (comment_id, post_id, message, status) 
             VALUES ($1, $2, $3, $4) ON CONFLICT (comment_id) DO NOTHING`,
            [event.comment_id, event.post_id, event.message, 'received']
        );

        // Bước B: Kiểm tra bộ lọc Spam
        const spamStatus = checkSpamLogic(event.message);
        let action = 'reply';
        let replyText = 'Dạ shop xin chào bạn ạ!';

        if (spamStatus === 'spam_link') {
            action = 'hide'; // Spam chứa link độc hại -> ẩn ngay lập tức (Mục 4.2)
            replyText = '';
            console.log(`[AUTOMATION] Phát hiện link lạ -> Quyết định: HIDE (Ẩn bình luận).`);
            
            // Cập nhật trạng thái ẩn vào DB
            await db.query(`UPDATE comments SET status = $1 WHERE comment_id = $2`, ['hidden', event.comment_id]);
        } else {
            // Bước C: Nếu comment sạch, tiến hành phân tích cảm xúc qua AI
            const aiResult = await analyzeWithAI(event.message);
            console.log(`[AI] Kết quả phân loại -> Intent: ${aiResult.intent} | Sentiment: ${aiResult.sentiment}`);

            // Luật tự động hóa ra quyết định phản hồi (Mục 5.2)
            if (aiResult.sentiment === 'positive') {
                replyText = 'Cảm ơn bạn rất nhiều vì đã ủng hộ shop! Chúc bạn một ngày vui vẻ ❤️';
            } else if (aiResult.sentiment === 'negative') {
                replyText = 'Dạ thành thật xin lỗi bạn vì trải nghiệm chưa tốt. Shop sẽ có nhân viên liên hệ hỗ trợ mình ngay ạ.';
            } else if (aiResult.intent === 'ask_price') {
                replyText = 'Dạ shop đã gửi thông tin báo giá chi tiết vào tin nhắn inbox của mình rồi ạ!';
            }

            // Cập nhật kết quả phân tích AI và chuyển trạng thái sang 'processed'
            await db.query(
                `UPDATE comments SET intent = $1, sentiment = $2, status = $3 WHERE comment_id = $4`,
                [aiResult.intent, aiResult.sentiment, 'processed', event.comment_id]
            );
        }

        // Bước D: Đẩy quyết định hành động sang topic 'reply_commands' để Backend API tiêu thụ
        const commandPayload = {
            schema_version: 1,
            command_id: `cmd_${Date.now()}`,
            event_id: event.event_id,
            action: action,
            page_id: event.page_id,
            comment_id: event.comment_id,
            user_id: event.user_id,
            user_name: event.user_name,
            reply_text: replyText,
            intent: spamStatus === 'spam_link' ? 'spam' : 'analyzed',
            sentiment: spamStatus === 'spam_link' ? 'neutral' : 'analyzed',
            created_at: new Date().toISOString()
        };

        await producer.send({
            topic: 'reply_commands',
            messages: [{ value: JSON.stringify(commandPayload) }]
        });
        console.log(`[KAFKA] Đã publish lệnh xử lý sang topic 'reply_commands'.`);

    } catch (error) {
        console.error('[LỖI XỬ LÝ SỰ KIỆN CORES_SERVICE]', error);
    }
}

// Khởi chạy dịch vụ tiêu thụ hàng đợi
async function start() {
    await db.connect();
    await consumer.connect();
    await producer.connect();

    // Consume dữ liệu từ topic đầu vào raw_events
    await consumer.subscribe({ topic: 'raw_events', fromBeginning: false });
    console.log(' Core Service đang lắng nghe dữ liệu từ Kafka...');

    await consumer.run({
        eachMessage: async ({ message }) => {
            const event = JSON.parse(message.value.toString());
            await handleEvent(event);
        },
    });
}

start().catch(console.error);