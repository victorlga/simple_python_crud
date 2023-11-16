from fastapi import FastAPI
import os
import mysql.connector

app = FastAPI()



def create_connection():
    database_host = os.getenv("DATABASE_HOST")
    connection = mysql.connector.connect(
        host=database_host,
        user="username",
        password="super_secret_password",
        database="mysql_db"
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
