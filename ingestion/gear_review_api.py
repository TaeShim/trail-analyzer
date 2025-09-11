import praw
import os
from dotenv import load_dotenv
import pandas as pd
import re, unicodedata
from transformers import pipeline
import dbutils

# Get secrets from Databricks
REDDIT_USER_AGENT   = dbutils.secrets.get("trailanalyzer-dev", "REDDIT_USER_AGENT")
REDDIT_CLIENT_ID    = dbutils.secrets.get("trailanalyzer-dev", "REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET= dbutils.secrets.get("trailanalyzer-dev", "REDDIT_CLIENT_SECRET")

print("Successfully retrieved Reddit API credentials from Databricks secrets")

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,    
    user_agent=REDDIT_USER_AGENT
)

outdoor_gear_brands = [
    # Big general outdoor brands
    "Patagonia", "North Face", "Columbia", "Arc'teryx", "Arc", "Mountain Hardwear",
    "Marmot", "Outdoor Research", "REI", "Eddie Bauer", "LL Bean",
    "Helly Hansen", "Jack Wolfskin", "Cotopaxi", "Rab", "Montbell",

    # Footwear
    "Merrell", "Salomon", "La Sportiva", "Scarpa", "Oboz", "Altra",
    "Hoka", "Keen", "Vasque", "Lowa", "Brooks", "Inov-8",

    # Backpacks & bags
    "Osprey", "Gregory", "Deuter", "Granite Gear", "Hyperlite Mountain Gear",
    "ULA Equipment", "Gossamer Gear", "Mystery Ranch", "Seek Outside",

    # Sleeping systems
    "Therm-a-Rest", "Sea to Summit", "Big Agnes", "Nemo Equipment",
    "Western Mountaineering", "Feathered Friends", "Enlightened Equipment",

    # Tents & shelters
    "MSR", "Hilleberg", "Zpacks", "Tarptent", "Durston Gear", "Six Moon Designs",

    # Miscellaneous
    "Buff", "Darn Tough", "Smartwool", "Icebreaker", "Outdoor Voices",
    "Prana", "Fjällräven", "Tilley", "Sunday Afternoons", "Montbell"
]

subreddits = ["hikinggear", "CampingGear", "hiking", "backpacking", "Campingandhiking"]

weather_season_words = [
    # Seasons
    "summer", "winter", "fall", "autumn", "spring",
    "mid-summer", "midwinter", "early spring", "late fall",
    
    # Temperature / climate
    "warm weather", "hot weather", "cold weather",
    "heat", "hot", "chilly", "freezing",
    "mild", "humid",

    # Rain / moisture
    "rainy", "rain", "drizzle", "showers",
    "wet", "storm", "thunderstorm",

    # Snow / ice
    "snow", "snowy", "blizzard", "snowstorm",
    "ice", "icy", "hail",

    # Wind
    "wind", "windy",

    # Misc
    "recommendation", "recommendations"
]

hiking_gear_categories = [
    # Clothing - More specific terms first
    "hiking pants", "rain pants", "hiking shorts", "base layer", "thermal", "hiking shirt",
    "fleece", "softshell jacket", "rain jacket", "waterproof jacket", "vest",
    "hat", "beanie", "gloves", "mittens", "hiking socks", "gaiters",
    "pants", "shorts", "shirt", "jacket", "cap",  # Moved cap to end

    # Footwear
    "hiking boots", "trail runners", "trail running shoes", "hiking shoes",
    "water shoes", "sandals",

    # Packs & Bags
    "daypack", "hiking backpack", "backpack", "hydration pack",

    # Navigation & Safety
    "map", "compass", "gps", "gps device", "satellite communicator",
    "garmin inreach", "headlamp", "flashlight", "first aid kit",
    "emergency blanket", "trekking poles", "bear spray",

    # Accessories
    "sunglasses", "multitool", "knife", "repair kit"
]
# ---------- text utils ----------
def normalize_text(s):
    s = s or ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("’", "'").replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def compile_brand_regex(brands, extras=None):
    # Common words that should not be treated as brand names
    excluded_words = {"or", "so", "and", "the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "by"}
    
    alts = set()
    for b in brands:
        w = b.lower()
        w_np = re.sub(r"[^\w\s]", "", w)
        alts.add(w)
        if w_np != w:
            alts.add(w_np)
    if extras:
        alts |= {e.lower() for e in extras}

    # Filter out excluded words
    alts = {a for a in alts if a not in excluded_words}

    parts = []
    for a in alts:
        if " " in a:
            parts.append(r"\b" + re.escape(a).replace(r"\ ", r"\s+") + r"\b")
        else:
            parts.append(r"\b" + re.escape(a) + r"\b")
    
    return re.compile("|".join(parts), re.IGNORECASE)

def brand_acronym(brands):
    brand_aliases = []
    for brand in brands:
        words = brand.split()
        if len(words) > 1:
            acronym = "".join(w[0] for w in words if w).lower()
            if acronym:
                brand_aliases.append(acronym)
    return brand_aliases

def extract_context_window(text, match, window_words=12):
    """
    Extract context window around a brand mention for sentiment analysis.
    
    Args:
        text: Full text containing the brand mention
        match: Regex match object for the brand
        window_words: Number of words to include on each side of the brand
    
    Returns:
        str: Context window with brand mention in the middle
    """
    s, e = match.start(), match.end()
    left = text[:s]
    mid = match.group(0)
    right = text[e:]

    word_re = r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)*"

    left_tokens = re.findall(word_re, left)
    right_tokens = re.findall(word_re, right)

    # Get exactly window_words on each side, or as many as available
    left_ctx = " ".join(left_tokens[-window_words:]) if len(left_tokens) >= window_words else " ".join(left_tokens)
    right_ctx = " ".join(right_tokens[:window_words]) if len(right_tokens) >= window_words else " ".join(right_tokens)

    # Build the context window
    if left_ctx and right_ctx:
        return f"{left_ctx} {mid} {right_ctx}"
    elif left_ctx:
        return f"{left_ctx} {mid}"
    elif right_ctx:
        return f"{mid} {right_ctx}"
    else:
        return mid

outdoor_gear_brands = [b.lower() for b in outdoor_gear_brands]

brand_re = compile_brand_regex(outdoor_gear_brands, extras=brand_acronym(outdoor_gear_brands))

def find_keyword(word_list, text):
    if not text:
        return None
    text_lower = text.lower()
    for word in word_list:
        pattern = r'\b' + re.escape(word) + r'\b'
        if re.search(pattern, text_lower):
            return word
    return None


_classifier = pipeline("sentiment-analysis",
                       model="cardiffnlp/twitter-roberta-base-sentiment-latest")

def get_sentiment(text, threshold, method):
    """
    Get sentiment with custom approaches to reduce neutrals.
    
    Args:
        text: Text to analyze
        threshold: Confidence threshold (0.0-1.0). Lower = more aggressive classification
        method: "threshold" or "weighted" - different approaches to reduce neutrals
    
    Returns:
        tuple: (label, score) where label is 'POSITIVE', 'NEGATIVE', or 'NEUTRAL'
    """
    res = _classifier(text, truncation=True, max_length=256)[0]
    label = res["label"]
    score = float(res["score"])
    
    if method == "threshold":
        # If confidence is below threshold, classify as neutral
        if score < threshold:
            return "NEUTRAL", score
        return label, score
    
    elif method == "weighted":
        # More aggressive: use weighted scoring to favor positive/negative
        # Get all possible labels and scores
        all_results = _classifier(text, truncation=True, max_length=256, top_k=None)
        
        # Find the highest scoring non-neutral label
        best_label = "NEUTRAL"
        best_score = 0.0
        
        for result in all_results:
            if result["label"] != "NEUTRAL" and result["score"] > best_score:
                best_label = result["label"]
                best_score = result["score"]
        
        # If we found a non-neutral with decent confidence, use it
        if best_score > 0.2:  # Very low threshold for non-neutral
            return best_label, best_score
        else:
            return "NEUTRAL", score
    
    elif method == "positive_bias":
        # Even more aggressive: heavily favor positive sentiment for gear recommendations
        # Get all possible labels and scores
        all_results = _classifier(text, truncation=True, max_length=256, top_k=None)
        
        # Find positive and negative scores
        positive_score = 0.0
        negative_score = 0.0
        neutral_score = 0.0
        
        for result in all_results:
            if result["label"] == "positive":
                positive_score = result["score"]
            elif result["label"] == "negative":
                negative_score = result["score"]
            elif result["label"] == "neutral":
                neutral_score = result["score"]
        
        # Heavily bias toward positive for gear recommendations
        # If positive is even slightly higher than neutral, choose positive
        if positive_score > neutral_score * 0.6:  # Even lower bar for positive
            return "positive", positive_score
        elif negative_score > neutral_score * 1.5:  # Much higher bar for negative
            return "negative", negative_score
        else:
            return "neutral", neutral_score
    
    else:
        # Default behavior
        return label, score

def create_df(window_words, sentiment_threshold, sentiment_method):
    data = []

    for name in subreddits:
        subreddit = reddit.subreddit(name)

        for submission in subreddit.hot(limit=400):
            if find_keyword(weather_season_words, submission.title):
                # Sort comments by top (most upvoted) before accessing them
                submission.comment_sort = "top"
                submission.comments.replace_more(limit=5)

                for comment in submission.comments.list():
                    body = comment.body or ""
                    norm = normalize_text(body)

                    weather_match = find_keyword(weather_season_words, body)
                    gear_match = find_keyword(hiking_gear_categories, body)

                    if not (weather_match and gear_match):
                        continue

                    for match in brand_re.finditer(norm):
                        ctx = extract_context_window(norm, match, window_words=window_words)
                        sent_label, sent_score = get_sentiment(ctx, threshold=sentiment_threshold, method=sentiment_method)

                        data.append({
                            "subreddit": name,
                            "sub_name": submission.title,
                            "sub_link": submission.url,
                            "comment_created": pd.to_datetime(comment.created_utc, unit="s"),
                            "comment_body": body,
                            "weather_season": weather_match,
                            "gear_type": gear_match,
                            "gear_brand": match.group(0).lower(),   # exact matched alias
                            "brand_context": ctx,               # text used for sentiment
                            "sent_label": sent_label,
                            "sent_score": sent_score,
                        })

    return pd.DataFrame(data)
