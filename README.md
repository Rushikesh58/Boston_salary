In my project, I implemented a robust data pipeline using Apache Airflow, Docker, and various cloud services for seamless data extraction, transformation, and visualization. Leveraging Docker, I containerized the Apache Airflow environment, ensuring consistency across different environments. 

The core of the data pipeline is an Apache Airflow DAG scheduled to run periodically every month. This DAG orchestrates the entire process, starting with data extraction from a REST API containing salary information for different professions in Boston. Once the data is extracted, it undergoes a rigorous cleaning process to ensure accuracy and consistency.

After the data cleansing step, the cleaned data is loaded into Google BigQuery, a powerful and scalable cloud-based data warehouse. Google BigQuery provides a reliable and efficient storage solution for the processed data, allowing for easy querying and analysis.

To visualize the data effectively, I integrated Google Looker and Tableau into the project. Google Looker offers intuitive and interactive data exploration tools, enabling in-depth analysis and visualization of the data stored in Google BigQuery. Additionally, Tableau, a popular data visualization tool, was employed to create rich and insightful visualizations, enhancing the overall data presentation and making it accessible to stakeholders.

By combining these technologies, the project not only ensures the seamless execution of the data pipeline but also provides a user-friendly interface for exploring and understanding the salary data, empowering data-driven decision-making within the organization. The use of Docker ensures the portability and easy deployment of the entire solution, making it scalable and adaptable to changing requirements.
