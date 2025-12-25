# E-Commerce Data Engineering Project (Week 2)

## 🚀 Project Overview
This project implements a professional **ETL Pipeline** (Extract, Transform, Load) for e-commerce data. We processed raw CSV files (orders and users) into a clean, analytics-ready format using Python and `uv`.

## 🛠️ Tech Stack
- **Language:** Python 3.11
- **Package Manager:** [uv](https://github.com/astral-sh/uv)
- **Libraries:** Pandas, Plotly, Pandera, PyArrow.
- **Environment:** Jupyter Notebook for EDA.

## 📈 Key Findings (from EDA)
- **Top Country:** [  ] with $[ ] total revenue.
- **Data Quality:** Achieved **100% join coverage** between orders and users.
- **Outliers:** Handled price outliers using **Winsorization** (1st/99th percentiles).

## 📂 Project Structure
- `src/`: Core logic (Transforms, Joins, Quality checks).
- `scripts/`: Production-ready ETL runner (`run_etl.py`).
- `notebooks/`: Exploratory Data Analysis (EDA) and Visualizations.
- `data/`: (Not pushed to Git) Raw and Processed data.

## ⚙️ How to Run
1. Install `uv`.
2. Run `uv sync` to set up the environment.
3. Execute the pipeline:
   ```bash
   uv run scripts/run_etl.py