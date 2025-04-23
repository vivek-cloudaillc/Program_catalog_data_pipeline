# Program Catalog Pipeline

This project is an **AWS Lambda-based web scraping pipeline** designed to extract and organize academic program information from [Old Dominion University (ODU)](https://catalog.odu.edu). It processes and stores structured data in AWS S3 and DynamoDB.

---

## 📌 Features

- Scrapes undergraduate and graduate program listings from ODU catalog
- Extracts course codes and department info from program detail tabs
- Downloads related program PDFs
- Uploads:
  - Raw HTML and PDF files to **S3**
  - Final structured program data as a JSON to **S3**
  - Structured records to **DynamoDB**

---

## 🧱 Architecture

- **Source:** ODU Program Catalog
- **Processing:** Python + Async + BeautifulSoup + Regex
- **Storage:** AWS S3 (HTML, PDF, JSON) and DynamoDB (structured data)
- **Execution:** AWS Lambda-compatible script

---

## 🚀 How It Works

1. Fetches program list from catalog
2. Visits each program page to scrape tabs and courses
3. Downloads PDFs and uploads to S3
4. Builds a final JSON file (`allprograms.json`)
5. Uploads JSON to S3 and populates DynamoDB

---

## 📂 AWS Configuration

- **S3 Bucket:** `cloudaillc-vivek`
  - `Program_Catalog_Pipeline/raw/`: Raw HTML pages
  - `Program_Catalog_Pipeline/program_pdfs/`: Program PDFs
  - `Program_Catalog_Pipeline/output/`: Final structured JSON
- **DynamoDB Table:** `program_data`

---

## 📦 Dependencies

See [`requirements.txt`](requirements.txt)

Install with:

```bash
pip install -r requirements.txt
