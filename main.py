import aiomysql
import asyncio
import boto3
import logging
import os
import time
from botocore.exceptions import ClientError, NoCredentialsError
from contextlib import asynccontextmanager
from fastapi import FastAPI

async def get_secret():
    secret_name = "app/mysql/credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    loop = asyncio.get_running_loop()

    try:
        # Unpack the dictionary into keyword arguments
        get_secret_value_response = await loop.run_in_executor(
            None,  # Uses the default executor
            lambda: client.get_secret_value(SecretId=secret_name)
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
import asyncio

async def push_logs_to_cloudwatch(log_message):
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,  # Uses the default executor (which is a ThreadPoolExecutor)
            lambda: log_client.put_log_events(
                logGroupName=LOG_GROUP,
                logStreamName=LOG_STREAM,
                logEvents=[
                    {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': log_message
                    },
                ],
            )
        )
    except Exception as e:
        logger.error(f"Error sending logs to CloudWatch: {e}")


async def create_users_table():
    conn = await create_connection()
    async with conn.cursor() as cursor:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255)
        );
        """
        try:
            await cursor.execute(create_table_query)
            await conn.commit()
        except Exception as e:
            print(f"Failed creating table: {e}")
            raise
        finally:
            conn.close()

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    # Startup logic: Creating users table
    await create_users_table()
    yield

app = FastAPI(lifespan=app_lifespan)

async def create_connection():
    secret = await get_secret()
    conn = await aiomysql.connect(
        host=os.getenv("DB_HOST"),
        user=secret["username"],
        password=secret["password"],
        db=secret["name"]
    )
    return conn


@app.post("/users/")
async def create_user(name: str, email: str):
    conn = await create_connection()
    async with conn.cursor() as cursor:
        query = "INSERT INTO users (name, email) VALUES (%s, %s)"
        values = (name, email)
        await cursor.execute(query, values)
        await conn.commit()
    
    conn.close()  # Ensure the connection is closed
    await push_logs_to_cloudwatch(f"Create user with name {name} and email {email}")

    return {"name": name, "email": email}

@app.get("/users/")
async def get_users():
    conn = await create_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM users")
        users = await cursor.fetchall()
    
    conn.close()  # Ensure the connection is closed
    await push_logs_to_cloudwatch("Get all users")

    return users

@app.put("/users/{user_id}")
async def update_user(user_id: int, name: str, email: str):
    conn = await create_connection()
    async with conn.cursor() as cursor:
        query = "UPDATE users SET name = %s, email = %s WHERE id = %s"
        values = (name, email, user_id)
        await cursor.execute(query, values)
        await conn.commit()
    
    conn.close()  # Ensure the connection is closed
    await push_logs_to_cloudwatch(f"Update user by id: {user_id}. Name: {name}. Email: {email}")

    return {"id": user_id, "name": name, "email": email}


@app.delete("/users/{user_id}")
async def delete_user(user_id: int):
    conn = await create_connection()
    async with conn.cursor() as cursor:
        query = "DELETE FROM users WHERE id = %s"
        await cursor.execute(query, (user_id,))
        affected_rows = cursor.rowcount  # Get the number of rows affected
        await conn.commit()
    
    conn.close()  # Ensure the connection is closed
    if affected_rows > 0:
        await push_logs_to_cloudwatch(f"Delete user by id: {user_id}")
        return {"status": "User deleted"}
    else:
        return {"status": "User not found"}


