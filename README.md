# Data Engineer Technique
## ⚙️ SCD (Slowly Changing Dimention):
### 📝 Khái niệm:
- SCD viết tắt cho Slowly Changing Dimension, là một kỹ thuật trong mô hình hóa dữ liệu, được sử dụng trong kho dữ liệu để xử lý các thay đổi theo thời gian trong các thuộc tính của dữ liệu. 

#### **Xem thêm lý thuyết về các loại SCD (nguyên lý hoạt động, ưu điểm và nhược điểm):** ***[Chi tiết các loại SCD](https://docs.google.com/document/d/1Y8w0AFGf5DL2vO3uhZEdpwIbg7wc5RlpXnPq2rq10Fs/edit?usp=sharing)***

### 📌 Source Code Demo các loại SCD: ***[SCD Demo Code](./SCD_demo)***

### 📌 Hướng dẫn chạy các code SCD demo: ***[Run_code_tutorial](./how_to_run_code.md)***

## ⚙️ CDC (Change Data Capture):
### 📝 Khái niệm:
- CDC là một quy trình để xác định và theo dõi các thay đổi của dữ liệu.

#### **Xem thêm lý thuyết về các loại CDC (nguyên lý hoạt động, ưu điểm và nhược điểm):** ***[Chi tiết các loại CDC](https://docs.google.com/document/d/1Y8w0AFGf5DL2vO3uhZEdpwIbg7wc5RlpXnPq2rq10Fs/edit?usp=sharing)***

### 📌 Source Code Demo các loại SCD: ***(đang cập nhật)*** ***[CDC Demo Code](./CDC_demo)***

## ⚙️ Apache Iceberg:
### 📝 Khái niệm:
- Apache Iceberg là 1 định dạng bảng dữ liệu phân tán, giúp đơn giản hóa việc xử lý dữ liệu trên các tập dữ liệu lớn được lưu trữ trong các kho dữ liệu.

#### **Xem thêm lý thuyết về Iceberg (định nghĩa, cấu trúc quản lý của dữ liệu bảng Iceberg,...):** ***[Chi tiết lý thuyết về Apache IceBerg](https://docs.google.com/document/d/1Y8w0AFGf5DL2vO3uhZEdpwIbg7wc5RlpXnPq2rq10Fs/edit?usp=sharing)***
### 📌 Source Code Demo ứng dụng Apache Iceberg: ***(đang cập nhật)*** ***[Iceberg Demo Code](./CDC_demo)***

## ⚙️ Build data pipeline:
### 📝 Khái niệm: 
- Luồng dữ liệu (Data Pipeline) là một hệ thống hoặc chuỗi các tiến trình tự động được thiết lập để di chuyển, chuyển đổi (transform) và tải (load) dữ liệu từ các hệ thống nguồn đến một kho lưu trữ đích (như Data Warehouse, Data Lake), nhằm mục đích chuẩn bị dữ liệu cho việc phân tích, báo cáo, và các ứng dụng Machine Learning.

![data_pipeline](/image/data_pipeline.jpg)

### 🔎 Phân tích Các Layer (Layered Architecture)
| No | Layer Name | Main function |
| :--- | :--- | :--- |
| 1 | Raw data /Ingestion / Data Source | Nơ lưu trữ dữ liệu tho (raw data) |
| 2 | Staging/ Bronze Layer | Đẩy 1:1 từ nguồn (nguyên vẹn): Dữ liệu được tải về và lưu trữ chính xác như khi lấy từ nguồn (không thực hiện bất kỳ thay đổi nào). |
| 3 | Processing/ Silver Layer | Làm sạch, Chuẩn hóa, Transform: Xử lý giá trị NULL, loại bỏ trùng lặp, chuẩn hóa kiểu dữ liệu. Đồng thời thực hiện xử lý logic (transform) cho dữ liệu |
| 4 | Curated / Gold Layer | Chuyển đổi Hoàn toàn & Áp dụng Logic Nghiệp vụ: Liên kết các bảng, tính toán chỉ số, áp dụng SCD. |
| 5 | Data Mart | Dữ liệu được tổng hợp, xử lý cho từng nghiệp vụ cụ thể |

***Ở tầng Processing và tầng Curated đều thực hiện việc chuyển đổi (transform) vậy có sự khác nhau gì ở 2 tầng?***

**Trong thực tế các dự án mình đã làm qua thì ví dụ rằng ta muốn tổng hợp 1 bảng và bảng đó dữ liệu được union từ việc xử lý logic của 2 hay nhiều bảng khác, thì ở tầng Processing ta sẽ tiến hành xử lý logic cho từng phần nhỏ, sau đó tầng curated ta sẽ tiến hành Union các bảng đã được xử lý đó lại và thiết lập xử lý SCD (nếu cần) cho bảng để được bảng hoàn thiện ở tầng Curated**

### 📌 Source Code Demo thiết lập Data Pipeline (Phân làm 4 tầng: stg, prc, cur, dmt): ***(đang cập nhật)*** ***[Data Pipeline Demo Code](./data_pipeline_demo)***