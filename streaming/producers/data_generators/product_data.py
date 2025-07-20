import random
from datetime import datetime, timedelta

PRODUCT_CATEGORIES = [
    "Figure", "T-Shirt", "Mug", "Poster", "Keychain", 
    "Manga", "Sticker", "Hoodie", "Cap", "Bag", "Phone Case", "Plushie"
]

ANIME_SERIES = [
    "One Piece", "Naruto", "Attack on Titan", "Demon Slayer", 
    "My Hero Academia", "Dragon Ball Z", "Bleach", "Hunter x Hunter",
    "Fullmetal Alchemist", "Death Note", "Tokyo Ghoul", "Jujutsu Kaisen",
    "Spy x Family", "Chainsaw Man", "Mob Psycho 100", "One Punch Man"
]

SUPPLIERS = ["SUP001", "SUP002", "SUP003", "SUP004", "SUP005"]

MATERIALS = {
    "Figure": ["PVC", "ABS Plastic", "Resin"],
    "T-Shirt": ["Cotton", "Polyester", "Cotton Blend"],
    "Mug": ["Ceramic", "Stainless Steel", "Glass"],
    "Poster": ["Paper", "Canvas", "Vinyl"],
    "Keychain": ["Metal", "Plastic", "Rubber"],
    "Manga": ["Paper"],
    "Sticker": ["Vinyl", "Paper"],
    "Hoodie": ["Cotton", "Fleece", "Cotton Blend"],
    "Cap": ["Cotton", "Polyester", "Canvas"],
    "Bag": ["Canvas", "Polyester", "Nylon"],
    "Phone Case": ["Silicone", "Plastic", "TPU"],
    "Plushie": ["Plush Fabric", "Cotton Fill"]
}

COLORS = ["Red", "Blue", "Black", "White", "Green", "Yellow", "Purple", "Pink", "Orange", "Multi-Color"]

class ProductDataGenerator:
    
    @staticmethod
    def generate_product():
        """Generate a product with realistic data"""
        category = random.choice(PRODUCT_CATEGORIES)
        anime = random.choice(ANIME_SERIES)
        
        # Price ranges based on category
        price_ranges = {
            "Figure": (25.99, 299.99),
            "T-Shirt": (15.99, 45.99),
            "Mug": (9.99, 24.99),
            "Poster": (5.99, 19.99),
            "Keychain": (3.99, 15.99),
            "Manga": (7.99, 16.99),
            "Sticker": (1.99, 9.99),
            "Hoodie": (29.99, 79.99),
            "Cap": (12.99, 29.99),
            "Bag": (19.99, 59.99),
            "Phone Case": (8.99, 34.99),
            "Plushie": (15.99, 89.99)
        }
        
        min_price, max_price = price_ranges.get(category, (5.99, 50.99))
        
        # Generate realistic dimensions based on category
        dimensions = ProductDataGenerator._generate_dimensions(category)
        
        # Generate SKU
        sku = f"{anime[:3].upper()}-{category[:3].upper()}-{random.randint(1000, 9999)}"
        
        return {
            "product_id": f"PROD{random.randint(10000, 99999)}",
            "sku": sku,
            "product_name": f"{anime} {category}",
            "category": category,
            "anime_series": anime,
            "price": round(random.uniform(min_price, max_price), 2),
            "cost": round(random.uniform(min_price * 0.4, min_price * 0.7), 2),  # 40-70% of price
            "supplier_id": random.choice(SUPPLIERS),
            "stock_quantity": random.randint(0, 500),
            "reorder_level": random.randint(10, 50),
            "weight": round(random.uniform(0.05, 5.0), 2),
            "dimensions": dimensions,
            "color": random.choice(COLORS),
            "material": random.choice(MATERIALS.get(category, ["Unknown"])),
            "is_limited_edition": random.choice([True, False]) if random.random() < 0.15 else False,
            "is_pre_order": random.choice([True, False]) if random.random() < 0.1 else False,
            "release_date": ProductDataGenerator._generate_release_date(),
            "age_rating": random.choice(["All Ages", "Teen", "Mature"]),
            "origin_country": random.choice(["Japan", "China", "South Korea"]),
            "warranty_months": random.choice([0, 6, 12, 24]),
            "tags": ProductDataGenerator._generate_tags(anime, category),
            "description": f"Official {anime} {category.lower()} featuring high-quality design and authentic details."
        }
    
    @staticmethod
    def _generate_dimensions(category):
        """Generate realistic dimensions based on product category"""
        dimension_ranges = {
            "Figure": {"length": (10, 30), "width": (8, 25), "height": (15, 40)},
            "T-Shirt": {"length": (60, 80), "width": (40, 60), "height": (1, 2)},
            "Mug": {"length": (12, 15), "width": (12, 15), "height": (8, 12)},
            "Poster": {"length": (30, 100), "width": (20, 70), "height": (0.1, 0.2)},
            "Keychain": {"length": (3, 8), "width": (2, 6), "height": (0.5, 2)},
            "Manga": {"length": (18, 21), "width": (12, 15), "height": (1, 3)},
            "Sticker": {"length": (5, 20), "width": (5, 20), "height": (0.1, 0.2)},
            "Hoodie": {"length": (65, 85), "width": (50, 70), "height": (2, 3)},
            "Cap": {"length": (25, 30), "width": (20, 25), "height": (10, 15)},
            "Bag": {"length": (30, 50), "width": (20, 40), "height": (10, 25)},
            "Phone Case": {"length": (14, 18), "width": (7, 10), "height": (1, 2)},
            "Plushie": {"length": (15, 50), "width": (10, 40), "height": (10, 35)}
        }
        
        ranges = dimension_ranges.get(category, {"length": (5, 30), "width": (5, 30), "height": (1, 10)})
        
        length = round(random.uniform(*ranges["length"]), 1)
        width = round(random.uniform(*ranges["width"]), 1)
        height = round(random.uniform(*ranges["height"]), 1)
        
        return f"{length}x{width}x{height}cm"
    
    @staticmethod
    def _generate_release_date():
        """Generate a release date (past, present, or future)"""
        # 70% past releases, 20% current, 10% future
        rand = random.random()
        
        if rand < 0.7:  # Past release
            start_date = datetime.now() - timedelta(days=365*2)  # Up to 2 years ago
            end_date = datetime.now() - timedelta(days=1)
        elif rand < 0.9:  # Current releases (within last month)
            start_date = datetime.now() - timedelta(days=30)
            end_date = datetime.now()
        else:  # Future releases
            start_date = datetime.now() + timedelta(days=1)
            end_date = datetime.now() + timedelta(days=180)  # Up to 6 months future
        
        random_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        return random_date.isoformat()
    
    @staticmethod
    def _generate_tags(anime, category):
        """Generate relevant tags for the product"""
        base_tags = [anime.lower().replace(" ", "-"), category.lower()]
        
        additional_tags = [
            "collectible", "official", "merchandise", "anime", "manga",
            "japanese", "otaku", "cosplay", "gift", "authentic"
        ]
        
        # Add 2-4 additional tags
        selected_tags = random.sample(additional_tags, k=random.randint(2, 4))
        
        return base_tags + selected_tags
    
    @staticmethod
    def get_random_product_id():
        """Get a random existing product ID"""
        return f"PROD{random.randint(10000, 99999)}"
    
    @staticmethod
    def get_seasonal_products():
        """Get products based on current season"""
        month = datetime.now().month
        
        if month in [12, 1, 2]:  # Winter
            return ["Hoodie", "Cap", "Mug", "Plushie"]
        elif month in [3, 4, 5]:  # Spring
            return ["T-Shirt", "Bag", "Poster", "Manga"]
        elif month in [6, 7, 8]:  # Summer
            return ["T-Shirt", "Cap", "Bag", "Sticker"]
        else:  # Fall
            return ["Hoodie", "Figure", "Manga", "Phone Case"]
    
    @staticmethod
    def get_trending_anime():
        """Get currently trending anime series"""
        trending = [
            "Spy x Family", "Chainsaw Man", "Jujutsu Kaisen", 
            "Attack on Titan", "Demon Slayer", "One Piece"
        ]
        return random.choice(trending)
    
    @staticmethod
    def calculate_profit_margin(cost, price):
        """Calculate profit margin percentage"""
        if price <= 0:
            return 0
        return round(((price - cost) / price) * 100, 2)
    
    @staticmethod
    def generate_batch_products(count=100):
        """Generate a batch of products for initial data load"""
        products = []
        for _ in range(count):
            products.append(ProductDataGenerator.generate_product())
        return products