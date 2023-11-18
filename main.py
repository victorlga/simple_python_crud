from fastapi import FastAPI
import os
import mysql.connector
import logging
import time
import boto3
from botocore.exceptions import NoCredentialsError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Create a CloudWatch log client
try:
    log_client = boto3.client('logs', region_name="us-east-1")
except NoCredentialsError:
    logger.error("AWS credentials not found")

LOG_GROUP = '/my-fastapi-app/logs'
LOG_STREAM = os.getenv("INSTANCE_ID")

# Function to push logs to CloudWatch
def push_logs_to_cloudwatch(log_message):
    try:
        log_client.put_log_events(
            logGroupName=LOG_GROUP,
            logStreamName=LOG_STREAM,
            logEvents=[
                {
                    'timestamp': int(round(time.time() * 1000)),
                    'message': log_message
                },
            ],
        )
    except Exception as e:
        logger.error(f"Error sending logs to CloudWatch: {e}")

# Use this function to log messages
push_logs_to_cloudwatch("Your log message here")

def put_custom_metric(metric_name, value):
    cloudwatch = boto3.client('cloudwatch', region_name='YOUR_AWS_REGION') # Replace with your region
    cloudwatch.put_metric_data(
        Namespace='YourApplication',
        MetricData=[
            {
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count'
            },
        ]
    )



app = FastAPI()

def create_connection():
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME")
    )
    return connection

def create_users_table():
    connection = create_connection()
    cursor = connection.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255)
    );
    """
    try:
        cursor.execute(create_table_query)
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
        raise
    finally:
        cursor.close()
        connection.close()

@app.on_event("startup")
async def startup_event():
    create_users_table()

@app.post("/users/")
def create_user(name: str, email: str):
    connection = create_connection()
    cursor = connection.cursor()
    query = "INSERT INTO users (name, email) VALUES (%s, %s)"
    values = (name, email)
    cursor.execute(query, values)
    connection.commit()
    cursor.close()
    connection.close()
    put_custom_metric('CreateUser', 1)
    return {"name": name, "email": email}

@app.get("/users/")
def get_users():
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()
    cursor.close()
    connection.close()
    put_custom_metric('GetUsers', 1)
    return users

@app.put("/users/{user_id}")
def update_user(user_id: int, name: str, email: str):
    connection = create_connection()
    cursor = connection.cursor()
    query = "UPDATE users SET name = %s, email = %s WHERE id = %s"
    values = (name, email, user_id)
    cursor.execute(query, values)
    connection.commit()
    cursor.close()
    connection.close()
    put_custom_metric('UpdateUser', 1)
    return {"id": user_id, "name": name, "email": email}

@app.delete("/users/{user_id}")
def delete_user(user_id: int):
    connection = create_connection()
    cursor = connection.cursor()
    query = "DELETE FROM users WHERE id = %s"
    cursor.execute(query, (user_id,))
    connection.commit()
    cursor.close()
    connection.close()
    put_custom_metric('DeleteUser', 1)
    return {"status": "User deleted"}
