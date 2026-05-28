# FP MCI — Timeline & Milestone Plan

**Deadline:** 10 Juni 2026  
**Constraint:** 30–31 Mei tidak bisa coding / pegang laptop.

---

# Phase 1 — Data Understanding & Architecture

📅 **28–29 Mei**

## Objective
Finalisasi scope analytics, dataset, dan desain pipeline.

## Deliverables

- [ ] Finalisasi analytics scope
- [ ] Dataset selection final
- [ ] Dataset relationship mapping
- [ ] ERD / join plan sederhana
- [ ] Project architecture draft
- [ ] Folder structure final

## Fokus

- memahami relasi antar dataset
- menentukan join strategy
- menentukan KPI analytics
- menentukan feature engineering utama

## Output Minimum

Join dasar berhasil:

```text
orders + reviews + payments + customers
```

---

# Phase 2 — Data Pipeline & Cleaning

📅 **1–2 Juni**

*(30–31 Mei skip total)*

## Objective

Membangun pipeline ingestion dan preprocessing.

## Deliverables

- [ ] fetch script stabil
- [ ] parquet/data lake berjalan
- [ ] Spark processing berjalan
- [ ] cleaned analytics dataset

## Feature Engineering Wajib

- [ ] delivery_delay_days
- [ ] repeat_customer_indicator
- [ ] avg_order_value
- [ ] purchase_count
- [ ] sentiment-ready review text

## Fokus

Belum perlu dashboard.

Prioritas utama:

```text
analytics dataset final sudah bersih
```

---

# Phase 3 — NLP & Sentiment Analysis

📅 **3–4 Juni**

## Objective

Menyelesaikan seluruh NLP pipeline.

## Deliverables

### Keyword Extraction

- [ ] KeyBERT keyword extraction

### Sentiment Analysis

- [ ] TextBlob / VADER sentiment score
- [ ] optional transformer comparison

### Complaint Topic Mining

- [ ] delivery complaints
- [ ] refund complaints
- [ ] damaged product complaints
- [ ] packaging complaints
- [ ] seller communication complaints

## Prioritas

### MUST

- [ ] KeyBERT
- [ ] Sentiment score

### OPTIONAL

- [ ] topic clustering otomatis

Jika waktu mepet:
manual keyword grouping masih acceptable.

---

# Phase 4 — Core Analytics & Insight Building

📅 **5–6 Juni**

## Objective

Menyelesaikan seluruh analytics utama.

## Deliverables

### MUST HAVE

- [ ] Delivery Impact Analysis
- [ ] Customer Segmentation
- [ ] NLP Review Analysis

### NICE TO HAVE

- [ ] Geo Analytics
- [ ] Payment Behavior Analysis
- [ ] Seller Proximity Analysis

## Fokus

Mulai menjawab:

```text
"So what?"
```

Bukan hanya membuat visualisasi.

Contoh pertanyaan insight:

- apakah delivery delay menurunkan review score?
- apakah sentiment positif berkorelasi dengan retention?
- siapa customer loyal?
- state mana paling bermasalah?
- apakah seller proximity mempengaruhi CX?

---

# Phase 5 — Dashboard & Visualization

📅 **7 Juni**

## Objective

Dashboard final usable.

## Deliverables

- [ ] dashboard final
- [ ] visual hierarchy clean
- [ ] filters berfungsi
- [ ] visual redundancy dikurangi

## Note

Lebih baik:

```text
8 chart bagus + insight kuat
```

daripada:

```text
20 chart random
```

---

# Phase 6 — README & Portfolio Polish

📅 **8 Juni**

## Objective

Merapikan project untuk GitHub portfolio.

## README Structure

- [ ] project overview
- [ ] business problem
- [ ] analytics objectives
- [ ] dataset description
- [ ] architecture pipeline
- [ ] folder structure
- [ ] preprocessing pipeline
- [ ] dashboard preview
- [ ] key insights
- [ ] tech stack
- [ ] future work

---

# Phase 7 — IEEE Paper Writing

📅 **8–9 Juni**

## Objective

Paper selesai 80–90%.

## Paper Structure

### 1. Introduction

- business background
- importance of customer experience analytics

### 2. Methodology

- datasets
- preprocessing
- NLP pipeline
- customer segmentation
- CX score methodology

### 3. Results & Discussion

- analytics findings
- dashboard interpretation
- insight discussion

### 4. Conclusion

- findings summary
- future improvement

## Fokus

Prioritaskan:

```text
jelas + runtut + insight-driven
```

---

# Phase 8 — Final Buffer & Debugging

📅 **10 Juni**

## Objective

Final checking.

## Checklist

- [ ] dashboard aman
- [ ] query aman
- [ ] README selesai
- [ ] paper formatting benar
- [ ] screenshot lengkap
- [ ] repo clean
- [ ] requirements.txt
- [ ] final testing

---

# Priority System

## Tier 1 — Wajib Ada

Jika waktu mepet, ini harus selesai.

- [ ] Delivery Impact Analysis
- [ ] Sentiment Analysis
- [ ] Customer Segmentation
- [ ] Dashboard
- [ ] README

---

## Tier 2 — Kalau Masih Ada Waktu

- [ ] Geo Analytics
- [ ] Payment Behavior
- [ ] Seller Proximity
- [ ] CX Score

---

## Tier 3 — Stretch Goal

- [ ] Transformer sentiment model
- [ ] Advanced topic clustering
- [ ] Interactive heatmap
- [ ] Advanced complaint mining

---

# Final Reminder

```text
Insight > Complexity
```

Project analytics yang kuat bukan yang:

- model paling rumit
- dashboard paling ramai

Tetapi yang punya:

- problem statement jelas
- pipeline rapi
- insight kuat
- stakeholder relevance
- storytelling yang bagus

SOURCE DATA
(CSV/API)

     ↓

fetch_dustiniadelixia_stream.py

     ↓

DATA LAKE

orders/
reviews/
customers/
products/

     ↓

process_dustiniadelixia_spark.py

cleaning
joins
feature engineering
analytics

     ↓

nlp_keyword_extraction.py

keyword extraction
review analysis

     ↓

warehouse/dashboard

     ↓

Metabase / PowerBI / dashboard

Saya sengaja menjaga scope tetap customer-centric. Namun dataset ini sebenarnya juga memungkinkan extension ke operational analytics seperti seller reliability atau fulfillment analysis apabila ingin memperluas stakeholder coverage.

