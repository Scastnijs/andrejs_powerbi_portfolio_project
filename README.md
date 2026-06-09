# andrejs_powerbi_portfolio_project
<p>
  Prerequisites

- Docker Desktop
- Power BI Desktop
- Python 3.12

Setup

1. git clone repository
2. docker compose up -d
3. create Python virtual environment
	python3 -m venv pbi_venv
	pbi_venv\Scripts\activate
4. pip install -r powerbi/python_requirements.txt
5. configure Power BI Python executable
6. open dashboard.pbix
7. click Refresh

Results

- Airflow loads CSV and API data
- PostgreSQL and MySQL store transformed data
- Power BI refreshes semantic model
- Python visuals regenerate automatically
</p>
