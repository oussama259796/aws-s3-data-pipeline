# AWS S3 Data Pipeline 🚀

I built this project to practice real data engineering using Python and AWS S3.
The idea is simple: take a raw CSV file, clean it up, then summarize it —
all stored in three separate S3 buckets following the Medallion Architecture (Bronze → Silver → Gold).

---

## ⚙️ Setup

Clone the repo and install dependencies:

```bash
pip install boto3 pandas python-dotenv
```

Create a `.env` file with your AWS credentials:

```text
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
```

Put your CSV file inside the `Data/` folder, then run:

```bash
python pipeline.py
```

---

## 📊 Output Example

After running the pipeline, you will see:

```text
📤 Uploaded: SalesData.csv
✨ Silver: 20994 clean rows
🏆 Gold:      9 aggregated rows
🚀 Pipeline completed successfully!
```

---

## 📂 Project Structure

```text
├── pipeline.py
├── Data/
│   └── SalesData.csv
├── .env
└── README.md
```

---

## 👤 Author

**Oussama** — Aspiring Data Engineer
Turning raw data into business insights with Python and AWS.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://www.linkedin.com/in/oussema-benkhaoua-8006582b5/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black)](https://github.com/oussama259796)