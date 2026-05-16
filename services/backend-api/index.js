require('dotenv').config();
const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json()); // Cho phép server đọc dữ liệu JSON gửi lên

const PORT = process.env.PORT || 3000;
const FB_PAGE_ID = process.env.FB_PAGE_ID;
const FB_ACCESS_TOKEN = process.env.FB_PAGE_ACCESS_TOKEN;
const FB_GRAPH_URL = 'https://graph.facebook.com/v26.0';

// Middleware giả lập ghi Log đầy đủ cho mọi Request (Yêu cầu bắt buộc)
app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] Nhận request: ${req.method} ${req.url}`);
    next();
});

// Middleware giả lập phân quyền đơn giản (Chỉ Admin mới được đăng bài)
const checkAdminRole = (req, res, next) => {
    const userRole = req.headers['x-user-role']; // Client gửi quyền lên qua Header
    if (userRole !== 'admin') {
        console.warn(`[CẢNH BÁO] Truy cập bị từ chối: Yêu cầu quyền admin nhưng nhận được '${userRole}'`);
        return res.status(403).json({ success: false, error: '403 Forbidden - Chỉ Admin mới có quyền này!' });
    }
    next();
};

// 1. API Lấy danh sách bài viết trên Page
app.get('/posts', async (req, res) => {
    try {
        console.log(`[LOG] Đang gọi Facebook API để lấy danh sách bài viết...`);
        
        // Backend đóng vai trò proxy, tự gọi sang Facebook hộ Client
        const response = await axios.get(`${FB_GRAPH_URL}/${FB_PAGE_ID}/feed`, {
            params: { access_token: FB_ACCESS_TOKEN }
        });

        // Phản hồi đã chuẩn hóa cấu hình thành công
        res.status(200).json({ success: true, data: response.data.data });
    } catch (error) {
        handleFacebookError(error, res);
    }
});

// 2. API Tạo bài viết mới lên Page (Yêu cầu quyền Admin)
app.post('/post', checkAdminRole, async (req, res) => {
    const { message } = req.body;
    if (!message) {
        return res.status(400).json({ success: false, error: 'Nội dung bài viết (message) không được để trống' });
    }

    try {
        console.log(`[LOG] Đang gọi Facebook API để đăng bài với nội dung: "${message.substring(0, 20)}..."`);
        
        const response = await axios.post(`${FB_GRAPH_URL}/${FB_PAGE_ID}/feed`, null, {
            params: {
                message: message,
                access_token: FB_ACCESS_TOKEN
            }
        });

        res.status(201).json({ success: true, data: response.data });
    } catch (error) {
        handleFacebookError(error, res);
    }
});

// 3. API Lấy danh sách bình luận của một bài viết cụ thể
app.get('/comments', async (req, res) => {
    const { post_id } = req.query; // Client truyền ?post_id=... trên URL
    if (!post_id) {
        return res.status(400).json({ success: false, error: 'Vui lòng cung cấp tham số post_id' });
    }

    try {
        console.log(`[LOG] Đang gọi Facebook API để lấy bình luận của bài viết ${post_id}...`);
        
        const response = await axios.get(`${FB_GRAPH_URL}/${post_id}/comments`, {
            params: { access_token: FB_ACCESS_TOKEN }
        });

        res.status(200).json({ success: true, data: response.data.data });
    } catch (error) {
        handleFacebookError(error, res);
    }
});

// Hàm hỗ trợ chuẩn hóa lỗi nhận về từ Facebook API
function handleFacebookError(error, res) {
    const fbError = error.response ? error.response.data.error : null;
    console.error(`[LỖI FACEBOOK API]`, fbError || error.message);

    if (fbError) {
        // Nếu Token hết hạn hoặc sai, Facebook thường trả về mã lỗi bảo mật cụ thể
        if (fbError.code === 190 || fbError.error_subcode === 463) {
            return res.status(401).json({ 
                success: false, 
                error: '401 Unauthorized - Facebook Access Token đã hết hạn hoặc không hợp lệ. Vui lòng cập nhật lại.' 
            });
        }
        // Các lỗi nghiệp vụ khác từ Facebook
        return res.status(error.response.status).json({ 
            success: false, 
            error: `Facebook Error: ${fbError.message}` 
        });
    }

    // Lỗi kết nối mạng hoặc lỗi hệ thống khác của chúng ta
    res.status(500).json({ success: false, error: 'Lỗi hệ thống nội bộ hoặc không thể kết nối tới Facebook.' });
}

// Khởi chạy server
app.listen(PORT, () => {
    console.log(`=======================================================`);
    console.log(` Server Backend API đang chạy thành công tại port ${PORT} `);
    console.log(`=======================================================`);
});