# E-Commerce Data Engineering Project (Week 2)

## 🚀 Project Overview
This project implements a professional **ETL Pipeline** (Extract, Transform, Load) for e-commerce data. We processed raw CSV files (orders and users) into a clean, analytics-ready format using Python and `uv`.

## 🛠️ Tech Stack
- **Language:** Python 3.11
- **Package Manager:** [uv](https://github.com/astral-sh/uv)
- **Libraries:** Pandas, Plotly, Pandera, PyArrow.
- **Environment:** Jupyter Notebook for EDA.

## 📊 Visual Analysis & Key Insights

### 1️⃣ Revenue by Country
![Revenue Chart](plots/revenue_by_country.png)
* **The Insight:** Market performance is balanced across the GCC. The **UAE (AE)** leads with the highest revenue (over **$320k**), followed by **Kuwait (KW)** and **Qatar (QA)**.
* **The Action:** Focus regional marketing strategies on the UAE to capitalize on its slight competitive edge.

### 2️⃣ Monthly Revenue Trends
![Revenue Over Time](plots/revenue_over_time.png)
* **The Insight:** Revenue peaked in **January ($106k)** but faced a sharp decline in **December (~$64k)**.
* **The Action:** Investigate the Q4 slump to determine if it's due to seasonal churn or supply chain issues.

### 3️⃣ Order Amount Distribution
![Order Distribution](plots/order_amount_distribution.png)
* **The Insight:** After **Winsorization**, order amounts show consistent volume between **$300 and $430**.
* **The Action:** Target high-value segments with "Premium Bundles" to increase Average Order Value (AOV).

## 🔍 Technical Findings & Data Quality (Caveats)
- **Data Integrity:** Achieved a **99.8% join success rate** between orders and users.
- **Cleanliness:** Standardized inconsistent status labels (e.g., "paid" vs "PAID").
- **Exclusions:** Dropped **473 records** (<10%) due to missing critical fields to ensure analysis accuracy.
- **Handling Outliers:** Applied **Winsorization** (1st/99th percentiles) to prevent price extremes from skewing results.

## 📂 Project Structure
- `src/`: Core logic (Transforms, Joins, Quality checks).
- `scripts/`: Production-ready ETL runner (`run_etl.py`).
- `notebooks/`: Exploratory Data Analysis (EDA) and Visualizations.
- `data/`: Raw and Processed data.

## ⚙️ How to Run
1. Install `uv`.
2. Run `uv sync` to set up the environment.
3. Execute the pipeline:
   ```bash
   uv run scripts/run_etl.py
## 📓 Exploratory Analysis (JupyterLab)
To explore the data and visualizations interactively, launch JupyterLab:
```bash
uv run jupyter lab