# Numismatic Market Analysis Pipeline
This project is a personal initiative born from a passion for banknote collecting. It implements a complete end-to-end Data Engineering pipeline to monitor and analyze the numismatic market, providing insights into the valuation of collectible currency.

## 📊 View Interactive Dashboard in Looker Studio
Explore the final data visualization, featuring dynamic filtering by century, historical status, and currency unit.

👉  https://datastudio.google.com/reporting/2f16c704-8a61-44f6-be76-600e93fc9615

## ❓ Problem Description
The numismatic market is highly dynamic, where the value of a banknote is influenced by multiple factors such as its country of origin, historical context (e.g., banknotes issued during wars), denomination, and physical condition.

As a collector, I identified a lack of centralized, historical data to track how these values evolve over time. This project solves that problem by:

* Market Monitoring: Scraping and consolidating data from numismatic markets to identify price trends.
* Factor Analysis: Determining which characteristics (Year, Condition, Rarity) have the highest impact on a banknote's price.
* Time-Series Tracking: Capturing periodic snapshots of market prices to visualize whether specific items are appreciating, depreciating, or stagnating over time.

This tool transforms raw market data into a structured Star Schema (Dimension and Fact tables) that enables professional-grade analytical reporting.

## 🏗️ Project Architecture
The pipeline follows the Modern Data Stack approach:

1. Infrastructure as Code (IaC): Terraform provisions the Google Cloud Storage (GCS) buckets and Google BigQuery datasets.
2. Orchestration: Managed by Prefect to coordinate the workflow.
3. Extraction & Transformation (Python): Custom asynchronous web scraper and complex data parser (detailed below).
4. Data Warehouse: Google BigQuery acts as the central repository, using external tables to read raw data.
5. Data Modeling: dbt (data build tool) handles dimensional modeling, creating a Star Schema (Dimension and Fact tables).
6. Automation: GitHub Actions runs the pipeline twice a month (1st and 15th).
7. Visualization: Looker Studio for interactive dashboards.

## 🖥️ Technologies Used

* Python 3.12 (Scraping and Logic)
* uv (Package and Project Management)
* Terraform (Infrastructure as Code)
* Google Cloud Platform (GCS and BigQuery)
* dbt-core (Transformations)
* Prefect Cloud (Orchestration)
* GitHub Actions (CI/CD and Scheduling)
* Looker Studio (Visualization)

## ⛏️ Data Extraction & Transformation Logic (Python)
A significant portion of this project relies on robust Python scripting to handle unstructured web data:

* Asynchronous Web Scraping: Using aiohttp and BeautifulSoup4, the get_raw_data.py script efficiently navigates pagination with semaphore limits to avoid server overloads while extracting raw HTML.
* Regex Parsing & Business Logic: The clean_raw_data.py script uses advanced Regular Expressions and dictionary mappings to extract details from messy product titles. It correctly identifies historical entities (e.g., distinguishing between existing countries and historical colonies like "Indochina Francesa"), normalizes currencies, and parses condition grades (UNC, XF, VF).
* Idempotency & Snapshot Tracking: The script generates a deterministic MD5 hash (generate_primary_key) for each unique banknote and a snapshot_id for every price extraction. This ensures the pipeline can run multiple times without duplicating core dimensions, while appending new historical prices accurately.

## 📖 Data Dictionary

This project utilizes a dimensional modeling approach (Star Schema) to organize the scraped numismatic data efficiently for analysis.

### 1. Fact Table: `fact_prices`
This table acts as the core of our analytical model. It captures the quantitative market data and tracks the valuation of banknotes over time, allowing for historical price analysis.

| Variable | Data Type | Description |
| :--- | :--- | :--- |
| `snapshot_id` | `STRING` | Unique identifier for each data extraction event, ensuring accurate time-series tracking and preventing data duplication. |
| `banknote_id` | `STRING` | Foreign key linking the price record to the specific banknote's descriptive attributes in the dimension table.  |
| `Price` | `FLOAT` | The market value of the banknote at the exact time of extraction. |
| `scraped_at` | `TIMESTAMP` | The exact timestamp when the price data was captured from the source website. |

### 2. Dimension Table: `dim_banknotes`
This dimension table stores all the descriptive, historical, and categorical attributes of the cataloged banknotes. It provides the necessary context to filter, group, and analyze the quantitative data found in the fact table.

| Variable | Data Type | Description |
| :--- | :--- | :--- |
| `banknote_id` | `STRING` | Primary key. A unique identifier for each distinct banknote model in the catalog. |
| `Country` | `STRING` | The original issuing country or territory of the banknote. |
| `Status` | `STRING` | The geopolitical status of the issuing entity, derived from historical mapping: Existent, Non-existent, Historical Colony. |
| `War` | `BOOLEAN` | A true/false flag indicating whether the banknote was issued during a significant historical conflict or wartime economy. |
| `Condition` | `STRING` | The numismatic grading of the banknote's physical state |
| `DenomValue` | `FLOAT` | The numerical face value printed on the banknote |
| `DenomUnit` | `STRING` | The currency denomination unit |
| `ExtraTags` | `STRING` | Additional numismatic characteristics or metadata extracted from the listing |
| `Century` | `INTEGER` | The century of issue, utilized for high-level historical grouping and macro-distribution analysis. |
| `Year` | `INTEGER` | The specific year of issue, allowing for granular chronological sorting and filtering. |


## 🛠️ Reproducibility
### Local Setup
To run this project on your local machine, follow these steps:

1. Prerequisites:

    * Install uv for Python dependency management.
    * Install Terraform.
    * A Google Cloud Project with a Service Account JSON key.

2. Clone the repository:

    1. git clone https://github.com/your-username/numismatic-analysis.git
    2. numismatic-analysis

3. Infrastructure Provisioning:
Navigate to your Terraform folder and apply the configuration to create the GCP resources:

    1. cd terraform
    2. terraform init
    3. terraform apply
    4. cd ..

4. Install dependencies:

    * uv sync

5. Environment Variables:
Create a .env file or export your GCP credentials and bucket names:

    1. export NUMISMATIC_BUCKET="your-gcs-bucket-name"
    2. export NUMISMATIC_RAW="raw_banknotes.csv"
    3. export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/gcp-key.json"

6. Run the Pipeline:

    1. uv run src/get_raw_data.py
    2. uv run src/clean_raw_data.py

7. Run dbt Transformations:

    1. cd transform_numismatic
    2. dbt build

### CI/CD Setup (GitHub Actions)
To automate the pipeline in the cloud:

1. Navigate to your GitHub Repository Settings > Secrets and variables > Actions.

2. Add the following Secrets:

    * GCP_SA_KEY: The entire content of your Google Service Account JSON key.
    * NUMISMATIC_BUCKET: The name of your GCS bucket.
    * PREFECT_API_KEY: Your Prefect Cloud API key.
    * PREFECT_WORKSPACE: Your Prefect workspace identifier.

3. The workflow is configured to run automatically on the 1st and 15th of every month, but you can trigger it manually from the Actions tab using the workflow_dispatch button.

## 🧪 Data Quality Tests
This project implements rigorous data testing via dbt:

* Uniqueness: Verified on banknote_id and snapshot_id.
* Integrity: not_null constraints on Year, Price, and IDs.
* Referential Integrity: Relationships tests to ensure every price in fact_prices corresponds to a valid entry in dim_banknotes.

## 🔮 Future Improvements
Given that the current parsing logic relies on Regular Expressions (Regex), the pipeline is sensitive to variations in how sellers format product titles. To improve the robustness and depth of the project, the following enhancements are proposed:

* Observability & Alerting: Implement a monitoring system using Prefect or GitHub Actions to send automatic notifications (via Slack or Email) in case of scraper failures or schema changes on the source website.

* Data Lineage Hosting: Automatically generate and host the dbt docs on GitHub Pages to provide a transparent, interactive view of the data transformations and dependencies.

* Anomaly Detection: Integrate the dbt-expectations package to identify and flag price outliers or significant market fluctuations that could indicate data entry errors or extreme rarity.

* Scraper Resilience: Implement rotating User-Agents and dynamic delays to ensure the pipeline remains resilient against anti-scraping measures as the volume of tracked pages increases.

## 🪐 About the Author
I am a Data Processing and Analytics Engineering student at the Universidad Nacional de Entre Ríos (UNER) in Argentina, with experience in Python, SQL, and data architecture. This project unites my technical skills in Data Engineering with my personal hobby of numismatics.

👩‍💻 Connect with me on LinkedIn: Camila Durand: https://www.linkedin.com/in/camila-ayelen-durand/
