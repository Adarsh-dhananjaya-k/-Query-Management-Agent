import os
import json
import logging
from datetime import datetime

# Setup log directory
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Setup text logger
log_file = os.path.join(LOG_DIR, "chat_history.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Chat Database (JSON acting as DB)
CHAT_DB_FILE = "chat_database.json"

def log_chat_interaction(user_info, user_message, ai_response):
    """
    Log chat to both text file and JSON 'database'.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_name = user_info.get("name", "Unknown")
    user_email = user_info.get("email", "Unknown")
    
    # 1. Log to text file
    logging.info(f"User: {user_name} ({user_email}) | Message: {user_message}")
    logging.info(f"AI: {ai_response}")
    logging.info("-" * 50)
    
    # 2. Log to JSON Database
    log_entry = {
        "timestamp": timestamp,
        "user_email": user_email,
        "user_name": user_name,
        "role": user_info.get("role"),
        "user_message": user_message,
        "ai_response": ai_response
    }
    
    db_data = []
    if os.path.exists(CHAT_DB_FILE):
        try:
            with open(CHAT_DB_FILE, "r", encoding="utf-8") as f:
                db_data = json.load(f)
        except:
            db_data = []
            
    db_data.append(log_entry)
    
    try:
        with open(CHAT_DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db_data, f, indent=4)
    except Exception as e:
        print(f"Failed to save chat to database: {str(e)}")
