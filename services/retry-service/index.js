const { Kafka } = require('kafkajs');

const kafka = new Kafka({ clientId: 'retry-service', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'retry-service-group' });
const producer = kafka.producer();

const MAX_RETRIES = 3; // Ngưỡng N lần thử lại tối đa theo đề bài yêu cầu

async function handleRetryLogic(failedMessage) {
    const currentRetry = failedMessage.retry_count + 1;
    console.log(`\n[RETRY PROCESSOR] Nhận tin nhắn lỗi của lệnh ${failedMessage.command_id}. Lần thử lỗi hiện tại: ${failedMessage.retry_count}`);

    if (currentRetry > MAX_RETRIES) {
        // KỊCH BẢN: Đã vượt quá ngưỡng N lần thử lại -> Đẩy vào Dead Letter Queue (DLQ)
        console.error(` [DLQ ALERT] Lệnh ${failedMessage.command_id} đã thất bại vượt quá ${MAX_RETRIES} lần! Đang chuyển vào Dead Letter Queue...`);
        
        const deadLetterPayload = {
            schema_version: 1,
            command_id: failedMessage.command_id,
            event_id: failedMessage.event_id,
            retry_count: failedMessage.retry_count,
            failed_at: new Date().toISOString(),
            final_error: failedMessage.last_error,
            original_topic: 'send_failed',
            payload: failedMessage.payload
        };

        await producer.send({
            topic: 'dead_letter',
            messages: [{ value: JSON.stringify(deadLetterPayload) }]
        });
        console.log(`[KAFKA] Đã chuyển tin nhắn lỗi vĩnh viễn sang topic 'dead_letter'. Thống kê lỗi kích hoạt Prometheus.`);
        return;
    }

    // KỊCH BẢN: Chưa đến ngưỡng tối đa -> Tính toán thời gian chờ tăng dần theo cấp số nhân (Exponential Backoff)
    // Công thức: 1s * 2^(retry_count)
    const backoffTimeMs = 1000 * Math.pow(2, failedMessage.retry_count); 
    console.log(`[BACKOFF] Áp dụng Exponential Backoff: Chờ đúng ${backoffTimeMs / 1000} giây trước khi gửi lại lệnh...`);

    // Chờ hết thời gian delay
    await new Promise(resolve => setTimeout(resolve, backoffTimeMs));

    // Đóng gói dữ liệu để Backend API thực hiện gửi lại thử nghiệm
    const retryPayload = {
        ...failedMessage,
        retry_count: currentRetry,
        next_retry_at: new Date(Date.now() + backoffTimeMs).toISOString()
    };

    await producer.send({
        topic: 'send_retry',
        messages: [{ value: JSON.stringify(retryPayload) }]
    });
    console.log(`[KAFKA] Đã đẩy tin nhắn thử lại sang topic 'send_retry' thành công.`);
}

async function start() {
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({ topic: 'send_failed', fromBeginning: false });
    console.log(' Retry Service đang chạy và lắng nghe topic send_failed...');

    await consumer.run({
        eachMessage: async ({ message }) => {
            const failedMessage = JSON.parse(message.value.toString());
            await handleRetryLogic(failedMessage);
        },
    });
}

start().catch(console.error);