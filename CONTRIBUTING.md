# Get started

  > sudo pip install requirements.txt

# Start Webserver

## To start webserver:
   
   > airflow webserver -p 8080

## To start webserver in debug mode:
   
   > airflow webserver -p 8080 -d

# Backfill some day range

  > airflow backfill get_orders_from_postgres_single -s 2015-08-02 -e 2015-08-02

# Run a task

  > airflow backfill get_orders_from_postgres_single s3_to_redshift 2015-08-05
 