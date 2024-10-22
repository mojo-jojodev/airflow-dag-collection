from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime

with DAG('postgres_s3_backup', schedule_interval='@daily', start_date=datetime(2024,1,1), catchup=False) as dag:
    
    pg_dump = BashOperator(
        task_id='execute_pg_dump',
        bash_command='pg_dump -U postgres -d app_db -F c -f /tmp/db_backup.dump'
    )

    upload_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename='/tmp/db_backup.dump',
        dest_key='backups/db_backup_{{ ds }}.dump',
        dest_bucket='enterprise-backups-bucket',
        aws_conn_id='aws_default',
        replace=True
    )

    clean_local = BashOperator(
        task_id='clean_local_file',
        bash_command='rm /tmp/db_backup.dump'
    )

    pg_dump >> upload_s3 >> clean_local
