# DustiniaDelixia Customer Performance Analytics

## 📌 Project Overview

DustiniaDelixia Customer Performance Analytics merupakan platform analitik pelanggan end-to-end yang dirancang untuk mengubah data transaksi e-commerce menjadi insight bisnis yang dapat digunakan dalam pengambilan keputusan.

Project ini membangun data pipeline otomatis mulai dari proses ingest data, transformasi, feature engineering, penyimpanan ke data warehouse, hingga visualisasi dashboard analytics menggunakan Apache Airflow, Apache Spark, ClickHouse, dan Metabase.

Melalui platform ini, perusahaan dapat memahami karakteristik pelanggan, mengevaluasi customer experience, menganalisis loyalitas pelanggan, serta mengidentifikasi faktor-faktor yang memengaruhi performa bisnis.

---

## 👨‍💻 Author

Ibrahim Ferel  
Department of Informatics Engineering  
Institut Teknologi Sepuluh Nopember (ITS)

---

# 🎯 Business Problem

Perusahaan e-commerce menghasilkan data transaksi dalam jumlah besar setiap harinya. Namun, data tersebut sering kali tersebar di berbagai sumber dan belum diolah menjadi informasi yang mudah digunakan untuk pengambilan keputusan.

Beberapa tantangan yang ingin diselesaikan melalui proyek ini:

- Sulit mengidentifikasi pelanggan loyal.
- Sulit memahami penyebab churn pelanggan.
- Belum tersedia customer experience measurement yang terukur.
- Sulit mengetahui faktor-faktor yang memengaruhi repeat order.
- Belum tersedia dashboard analitik terintegrasi untuk stakeholder bisnis.

---

# 🎯 Project Objectives

Proyek ini memiliki beberapa tujuan utama:

1. Membangun data pipeline end-to-end berbasis Airflow.
2. Mengotomatisasi proses ingest dan transformasi data.
3. Melakukan feature engineering untuk kebutuhan analytics.
4. Menyimpan analytical dataset pada ClickHouse.
5. Mengembangkan dashboard customer analytics.
6. Menghasilkan Customer Experience Score (CX Score).
7. Melakukan customer segmentation menggunakan pendekatan RFM.
8. Melakukan analisis review pelanggan menggunakan NLP.

---

# 📊 Dataset

Dataset yang digunakan berasal dari transaksi e-commerce dan terdiri dari beberapa tabel utama:

| Dataset | Description |
|----------|------------|
| orders | Informasi transaksi pelanggan |
| order_items | Detail item pada setiap transaksi |
| customers | Informasi pelanggan |
| sellers | Informasi seller |
| geolocation | Data lokasi pelanggan |
| order_reviews | Review pelanggan |
| order_payments | Informasi pembayaran |

---

# 🛠 Technology Stack

| Layer | Technology |
|---------|---------|
| Programming Language | Python |
| Orchestration | Apache Airflow |
| Distributed Processing | Apache Spark |
| Data Storage | Parquet |
| Data Warehouse | ClickHouse |
| Dashboarding | Metabase |
| NLP | KeyBERT, DistilBERT |
| Containerization | Docker |

---

# 🏗 System Architecture

text Google Drive Dataset         │         ▼ Fetch Layer         │         ▼ Raw Data Lake         │         ▼ Spark Processing Layer         │         ▼ Processed Data Layer         │         ▼ Feature Engineering Layer         │         ▼ ClickHouse Data Warehouse         │         ▼ Metabase Dashboard 

Tambahkan diagram architecture (.png/.jpg) pada bagian ini.

---

# 🔄 Data Pipeline Workflow

Pipeline dijalankan menggunakan Apache Airflow.

Tahapan utama pipeline:

1. Fetch Data
2. Process Data
3. Feature Engineering
4. Customer Experience Scoring
5. Load to ClickHouse
6. Dashboard Visualization

Pipeline berjalan secara otomatis melalui DAG Airflow.

Tambahkan screenshot DAG Airflow di sini.

---

# 🗄 Data Lake Design

text data_lake/ │ ├── raw/ │ ├── processed/ │ └── features/ 

---

## Raw Layer

Berisi dataset mentah yang telah dikonversi menjadi format Parquet.

text raw/ ├── orders ├── order_items ├── customers ├── sellers ├── geolocation ├── order_reviews └── order_payments 

---

## Processed Layer

Dataset hasil integrasi dan transformasi awal.

text processed/ ├── analytics_orders ├── customer_geo └── seller_proximity 

---

## Feature Layer

Dataset hasil feature engineering yang siap digunakan untuk analitik.

text features/ ├── customer_features ├── delivery_features ├── payment_features ├── nlp_features ├── seller_features └── cx_features 

---

# ⚙ Feature Engineering

## Customer Features

| Feature | Description |
|----------|------------|
| frequency | Total transaksi pelanggan |
| monetary | Total spending |
| avg_order_value | Nilai rata-rata transaksi |
| recency_days | Hari sejak transaksi terakhir |
| customer_tenure_days | Lama menjadi pelanggan |
| repeat_customer | Indikator repeat order |

---

## Delivery Features

| Feature | Description |
|----------|------------|
| actual_delivery_days | Lama pengiriman aktual |
| delivery_delay_days | Keterlambatan pengiriman |
| delay_bucket | Kategori keterlambatan |
| cancellation_flag | Status pembatalan |
| purchase_count_per_customer | Total transaksi customer |

---

## Payment Features

| Feature | Description |
|----------|------------|
| payment_type | Metode pembayaran |
| payment_installments | Jumlah cicilan |
| payment_count_per_order | Jumlah pembayaran |
| high_value_payment_flag | Indikator transaksi bernilai tinggi |

---

## NLP Features

| Feature | Description |
|----------|------------|
| review_text_clean | Teks review hasil preprocessing |
| complaint_topic | Kategori complaint |
| has_text | Review berisi teks atau tidak |

---

## Seller Features

| Feature | Description |
|----------|------------|
| same_state | Customer dan seller satu provinsi |
| interstate_order | Transaksi antar provinsi |
| freight_ratio | Rasio ongkir terhadap harga |
| freight_bucket | Kategori ongkir |

---

## CX Features

| Feature | Description |
|----------|------------|
| avg_review_score | Rata-rata rating |
| complaint_rate | Persentase complaint |
| late_delivery_rate | Persentase keterlambatan |
| cx_score | Customer Experience Score |
| cx_category | Segmentasi CX |

---

# ⭐ Customer Experience Score

Customer Experience Score (CX Score) digunakan untuk mengukur kualitas pengalaman pelanggan.

Komponen perhitungan:

text CX Score = 40% Review Score + 30% Delivery Performance + 20% Complaint Analysis + 10% Loyalty Score 

Kategori CX:

| Category | Score Range |
|----------|------------|
| Excellent | ≥ 90 |
| Good | 70 – 89 |
| Fair | 50 – 69 |
| Poor | < 50 |

---

# 📈 Dashboard Analytics

## Dashboard 1 — Customer Performance Overview

Menampilkan:

- Total Revenue
- Average Order Value
- Total Customer
- Revenue Trend
- Revenue by State

---

## Dashboard 2 — Customer Segmentation

Menampilkan:

- Frequency Analysis
- Monetary Analysis
- Recency Analysis
- Retention Segment
- Loyal Customer Segment

---

## Dashboard 3 — Customer Experience Analytics

Menampilkan:

- CX Score Distribution
- Review Distribution
- Complaint Topic Analysis
- Customer Experience Category

---

## Dashboard 4 — Customer Behavior Drivers

Menampilkan:

- Delivery vs Repeat Customer
- Payment vs Loyalty
- Customer Lifecycle Analysis

---

## Dashboard 5 — Geo & Seller Analytics

Menampilkan:

- Customer Distribution Map
- Revenue by State
- Seller Performance
- Freight Analysis

Tambahkan screenshot dashboard pada setiap section.

---

# 🤖 NLP Review Analytics

Review pelanggan dianalisis menggunakan pendekatan NLP.

Tahapan:

1. Text Cleaning
2. Text Normalization
3. Complaint Classification
4. Keyword Extraction (KeyBERT)
5. Sentiment Analysis (DistilBERT)

Output:

- Complaint Topic Classification
- Customer Sentiment
- Top Keywords
- Customer Feedback Insights

---

# 📏 Project KPI

Keberhasilan proyek diukur menggunakan KPI berikut:

| Program | Status |
|----------|----------|
| Data Engineering Pipeline | Completed |
| Customer Performance Dashboard | Completed |
| Customer Segmentation Dashboard | Completed |
| Customer Experience Dashboard | Completed |
| Customer Behavior Dashboard | Completed |
| Geo Analytics Dashboard | Completed |

---

# ⚠ Challenges & Solutions

## Challenge 1 — Customer Identity Mapping

Problem

customer_id tidak konsisten antar transaksi.

Solution

Menggunakan customer_unique_id sebagai customer master key.

---

## Challenge 2 — ClickHouse Datetime Loading

Problem

Error:

text NaTType does not support astimezone 

Solution

Melakukan konversi Spark Timestamp menjadi String sebelum proses insert ke ClickHouse.

---

## Challenge 3 — Feature Dependency Management

Problem

Beberapa feature bergantung pada hasil feature engineering lainnya.

Solution

Mengelola dependency menggunakan Apache Airflow DAG.

---

# 🚀 Future Improvements

Pengembangan lanjutan yang dapat dilakukan:

- Real-time Streaming Pipeline
- Churn Prediction Model
- Customer Lifetime Value Prediction
- Product Recommendation System
- Advanced Sentiment Analysis
- Customer Clustering menggunakan Machine Learning
- Real-Time Dashboard Monitoring

---

# 📚 References

- Apache Airflow Documentation
- Apache Spark Documentation
- ClickHouse Documentation
- Metabase Documentation
- KeyBERT Documentation
- HuggingFace Transformers Documentation

---

# 🤝 Acknowledgement

Project ini dikembangkan sebagai bagian dari implementasi Data Engineering, Data Analytics, dan Customer Intelligence Platform pada studi kasus e-commerce