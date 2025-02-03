# Project analysis of the GCP database


## Installation
1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo.git
   ```
2. Navigate to the project directory:
   ```sh
   cd your-repo
   ```
3. Install dependencies:
   ```sh
   pip install -r requirements.txt  # If using Python
   ```

`

## Files Overview

### from_google_query_jupyter.ipynb
This Jupyter Notebook is designed for querying Google BigQuery datasets and analyzing sensor data. It allows users to execute SQL queries, retrieve sensor statistics, and estimate query costs in an interactive environment.

With this notebook, you can:
- Connect to Google BigQuery using a service account.
- Retrieve key statistics from experiment datasets, including:
  - The number of distinct sensors.
  - The total number of data entries.
  - The latest recorded timestamp.
  - The estimated query cost in USD.
- Explore the query results in a Pandas DataFrame for further analysis.
- Modify queries and visualize data easily within Jupyter Notebook.

This tool is ideal for researchers and data analysts who need a flexible and interactive approach to exploring BigQuery datasets.

## Credentials
If you need a credentials file, please contact the admin:
- **Nir Averbuch**
- **Idan Ifrach**
- **Bnaya Hami**
- **menachem moshelion**

## Contributing
1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Commit your changes (`git commit -m 'Add new feature'`)
4. Push to the branch (`git push origin feature-branch`)
5. Open a pull request


