from fastapi import FastAPI, HTTPException
import os
import mysql.connector
from mysql.connector import errorcode

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
    return {"name": name, "email": email}

@app.get("/users/")
def get_users():
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()
    cursor.close()
    connection.close()
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
    return {"status": "User deleted"}

# To run the server:
# uvicorn main:app --reload
