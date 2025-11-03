from dotenv import load_dotenv
import os

load_dotenv()  # ensures .env variables are loaded in all terminals

print("Loaded DB_USER:", os.getenv("DB_USER"))
print("Loaded DB_PASSWORD:", os.getenv("DB_PASSWORD"))