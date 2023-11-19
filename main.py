import boto3
import logging
import mysql.connector
import os
import time
from botocore.exceptions import ClientError, NoCredentialsError
from contextlib import asynccontextmanager
from fastapi import FastAPI

def get_secret():

    secret_name = "app/mysql/credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    return eval(secret)

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

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    # Startup logic: Creating users table
    await create_users_table()
    yield

app = FastAPI(lifespan=app_lifespan)

def create_connection():
    secret = get_secret()
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=secret["username"],
        password=secret["password"],
        database=secret["name"]
    )
    return connection

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
    push_logs_to_cloudwatch(f"Create user with name {name} and email {email}")
    return {"name": name, "email": email}

@app.get("/users/")
def get_users():
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()
    cursor.close()
    connection.close()
    push_logs_to_cloudwatch("Get all users")
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
    push_logs_to_cloudwatch(f"Update user by id: {user_id}. Name: {name}. Email: {email}")
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
    push_logs_to_cloudwatch(f"Delete user by id: {user_id}")
    return {"status": "User deleted"}
