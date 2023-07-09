# Project: `Lushasa Web Scraping and MySQL database dumping project`

## Project Overview
`Website`: https://www.lushusa.com/home </br>
This project involves scraping data from a website and storing it in a MySQL database using Python,  BeautifulSoup, and Airflow. This project is running in `Docker Container`.

**Noted**
There is two file in this repository one is `Lushana_airflow_etl` in which I'll scrap data from website and save it in `csv` file then I'll read it and push it in the database. Second one is `Lushasa_etl_via_xcom` in which I'll scrap data from website and push it in `xcom` and pull it in another task and store it in MySQL db.


## Purpose
The purpose of this project is to automate the process of scraping data from a website and storing it in a database. By automating this process, we can save time and improve accuracy by eliminating the need for manual data entry.

## Technologies Used
The following technologies were used in this project:

1. Python
2. Beautiful Soup
3. MySQL Workbench
4. Airflow
5. Docker

**How It Works**
The project begins by scraping the website using python script written with the help of BeautifulSoup. The scraped data is then stored in a MySQL database using the `mysql.connector package`. Airflow is used to automate the entire process, including scheduling and monitoring the scraping and dumping of data.

# Conclusion
This project is an efficient and automated way to scrape data from a website and store it in a MySQL database. By automating the process, the project saves time and eliminates the need for manual data entry. The security measures taken ensure the safety and integrity of the scraped data.