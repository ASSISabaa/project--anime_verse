import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

CUSTOMER_SEGMENTS = ["VIP", "Premium", "Regular", "Casual"]
LOYALTY_TIERS = ["Platinum", "Gold", "Silver", "Bronze"]
PREFERRED_GENRES = ["Action", "Adventure", "Drama", "Comedy", "Romance", "Thriller", "Fantasy"]

class CustomerDataGenerator:
    
    @staticmethod
    def generate_customer_profile():
        """Generate a realistic customer profile"""
        segment = random.choice(CUSTOMER_SEGMENTS)
        
        # Loyalty tier based on segment
        if segment == "VIP":
            tier = random.choice(["Platinum", "Gold"])
        elif segment == "Premium":
            tier = random.choice(["Gold", "Silver"])
        else:
            tier = random.choice(["Silver", "Bronze"])
        
        return {
            "customer_id": f"CUST{random.randint(1000, 9999)}",
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "signup_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "segment": segment,
            "loyalty_tier": tier,
            "preferred_genres": "|".join(random.sample(PREFERRED_GENRES, k=random.randint(1, 3))),
            "birth_year": random.randint(1990, 2005),
            "country": fake.country(),
            "city": fake.city()
        }
    
    @staticmethod
    def generate_customer_behavior():
        """Generate customer behavior patterns"""
        return {
            "avg_monthly_spend": round(random.uniform(20, 500), 2),
            "visit_frequency": random.choice(["weekly", "monthly", "quarterly"]),
            "preferred_time": random.choice(["morning", "afternoon", "evening"]),
            "device_preference": random.choice(["mobile", "desktop", "tablet"]),
            "social_media_active": random.choice([True, False])
        }