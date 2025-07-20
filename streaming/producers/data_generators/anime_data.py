import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

ANIME_TITLES = [
    "One Piece", "Attack on Titan", "Demon Slayer", "My Hero Academia",
    "Dragon Ball Z", "Naruto", "Bleach", "Hunter x Hunter",
    "Fullmetal Alchemist", "Death Note", "Tokyo Ghoul", "Jujutsu Kaisen"
]

PRODUCT_CATEGORIES = ["Figure", "T-Shirt", "Mug", "Poster", "Keychain", "Manga", "Sticker"]
CHANNELS = ["online", "store", "mobile_app"]

def generate_customer_id():
    return f"CUST{random.randint(1000, 9999)}"

def generate_product_id():
    return f"PROD{random.randint(1000, 9999)}"

def generate_anime_id():
    return f"ANI{random.randint(100, 999)}"

def get_random_anime():
    return random.choice(ANIME_TITLES)

def get_random_product_category():
    return random.choice(PRODUCT_CATEGORIES)

def get_random_channel():
    return random.choice(CHANNELS)

def generate_timestamp():
    # Generate timestamp within the last 48 hours for late arrival simulation
    now = datetime.now()
    start_time = now - timedelta(hours=48)
    random_time = start_time + timedelta(
        seconds=random.randint(0, int((now - start_time).total_seconds()))
    )
    return random_time.isoformat()

def generate_recent_timestamp():
    # Generate very recent timestamp (last 5 minutes)
    now = datetime.now()
    start_time = now - timedelta(minutes=5)
    random_time = start_time + timedelta(
        seconds=random.randint(0, 300)
    )
    return random_time.isoformat()