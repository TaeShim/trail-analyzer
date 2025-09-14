import re
import unicodedata
import pandas as pd
import praw
from transformers import pipeline
import torch

# -------------------------------
# Lists of keywords and brands
# -------------------------------

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

# Subreddits where gear discussions are scraped
subreddits = ["hikinggear", "CampingGear", "hiking", "backpacking", "Campingandhiking"]

# Keywords related to weather/season conditions
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

# Categories of hiking gear to look for in comments
hiking_gear_categories = [
    # Clothing - more specific terms first
    "hiking pants", "rain pants", "hiking shorts", "base layer", "thermal", "hiking shirt",
    "fleece", "softshell jacket", "rain jacket", "waterproof jacket", "vest",
    "hat", "beanie", "gloves", "mittens", "hiking socks", "gaiters",
    "pants", "shorts", "shirt", "jacket", "cap",

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
    """Normalize text by removing odd characters and whitespace."""
    s = s or ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("’", "'").replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def compile_brand_regex(brands, extras=None):
    """
    Build a regex pattern to match brand names.
    Excludes common stopwords and supports multi-word brand names.
    """
    # Common words that should not be treated as brand names
    excluded_words = {"or", "so", "and", "the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "by"}
    
    alts = set()
    for b in brands:
        w = b.lower()
        w_np = re.sub(r"[^\w\s]", "", w)  # remove punctuation
        alts.add(w)
        if w_np != w:
            alts.add(w_np)
    if extras:
        alts |= {e.lower() for e in extras}

    # Filter out excluded words
    alts = {a for a in alts if a not in excluded_words}

    # Build regex parts
    parts = []
    for a in alts:
        if " " in a:  # multi-word brand (e.g., "mountain hardwear")
            parts.append(r"\b" + re.escape(a).replace(r"\ ", r"\s+") + r"\b")
        else:
            parts.append(r"\b" + re.escape(a) + r"\b")
    
    return re.compile("|".join(parts), re.IGNORECASE)

def brand_acronym(brands):
    """
    Generate lowercase acronyms for multi-word brand names.
    e.g., "Mountain Hardwear" → "mh"
    """
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
    Extract a context window around a matched brand mention.
    
    Args:
        text: Full text string
        match: Regex match object for the brand
        window_words: Number of words to include before/after brand
    
    Returns:
        str: A substring containing brand + nearby words
    """
    s, e = match.start(), match.end()
    left = text[:s]
    mid = match.group(0)
    right = text[e:]

    word_re = r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)*"

    left_tokens = re.findall(word_re, left)
    right_tokens = re.findall(word_re, right)

    # Take up to window_words tokens before and after
    left_ctx = " ".join(left_tokens[-window_words:]) if len(left_tokens) >= window_words else " ".join(left_tokens)
    right_ctx = " ".join(right_tokens[:window_words]) if len(right_tokens) >= window_words else " ".join(right_tokens)

    # Build the window string
    if left_ctx and right_ctx:
        return f"{left_ctx} {mid} {right_ctx}"
    elif left_ctx:
        return f"{left_ctx} {mid}"
    elif right_ctx:
        return f"{mid} {right_ctx}"
    else:
        return mid

# Convert brands to lowercase
outdoor_gear_brands = [b.lower() for b in outdoor_gear_brands]

# Build regex pattern for brands + acronyms
brand_re = compile_brand_regex(outdoor_gear_brands, extras=brand_acronym(outdoor_gear_brands))

def find_keyword(word_list, text):
    """Return first keyword found in text from given word list."""
    if not text:
        return None
    text_lower = text.lower()
    for word in word_list:
        pattern = r'\b' + re.escape(word) + r'\b'
        if re.search(pattern, text_lower):
            return word
    return None

# Load sentiment-analysis pipeline from HuggingFace Transformers
_classifier = pipeline("sentiment-analysis",
                       model="cardiffnlp/twitter-roberta-base-sentiment-latest")

def get_sentiment(text, threshold, method):
    """
    Classify sentiment of given text using chosen method.
    
    Args:
        text: String to analyze
        threshold: Confidence cutoff for 'threshold' method
        method: One of "threshold", "weighted", or "positive_bias"
    
    Returns:
        (label, score): label is 'POSITIVE'/'NEGATIVE'/'NEUTRAL'
    """
    res = _classifier(text, truncation=True, max_length=256)[0]
    label = res["label"]
    score = float(res["score"])
    
    if method == "threshold":
        # Use neutral if score < threshold
        if score < threshold:
            return "NEUTRAL", score
        return label, score
    
    elif method == "weighted":
        # Use alternative scoring to reduce neutral dominance
        all_results = _classifier(text, truncation=True, max_length=256, top_k=None)
        best_label, best_score = "NEUTRAL", 0.0
        for result in all_results:
            if result["label"] != "NEUTRAL" and result["score"] > best_score:
                best_label, best_score = result["label"], result["score"]
        return (best_label, best_score) if best_score > 0.2 else ("NEUTRAL", score)
    
    elif method == "positive_bias":
        # Favor positive scores when recommending gear
        all_results = _classifier(text, truncation=True, max_length=256, top_k=None)
        positive_score, negative_score, neutral_score = 0.0, 0.0, 0.0
        for result in all_results:
            if result["label"] == "positive":
                positive_score = result["score"]
            elif result["label"] == "negative":
                negative_score = result["score"]
            elif result["label"] == "neutral":
                neutral_score = result["score"]
        if positive_score > neutral_score * 0.6:
            return "positive", positive_score
        elif negative_score > neutral_score * 1.5:
            return "negative", negative_score
        else:
            return "neutral", neutral_score
    
    else:
        # Default: return original pipeline result
        return label, score

def create_df(window_words, sentiment_threshold, sentiment_method, client, secret, agent):
    """
    Scrape Reddit posts and comments for outdoor gear mentions,
    then run sentiment analysis on matched context.
    
    Args:
        window_words: # of context words around brand mentions
        sentiment_threshold: cutoff for neutral classification
        sentiment_method: one of the custom sentiment methods
        client, secret, agent: Reddit API credentials
    
    Returns:
        pd.DataFrame with columns:
          subreddit, sub_name, sub_link, comment_created, comment_body,
          weather_season, gear_type, gear_brand, brand_context,
          sent_label, sent_score
    """
    reddit = praw.Reddit(
        client_id=client,
        client_secret=secret,
        user_agent=agent,
    )

    data = []

    for name in subreddits:
        # Get subreddit object
        subreddit = reddit.subreddit(name)

        # Iterate over top 400 hot submissions
        for submission in subreddit.hot(limit=400):
            # Skip if no weather/season keyword in title
            if not find_keyword(weather_season_words, submission.title):
                continue

            submission.comment_sort = "top"
            submission.comments.replace_more(limit=0)  # load all top-level comments

            for comment in submission.comments.list():
                body = comment.body or ""
                norm = normalize_text(body)

                # Require both weather and gear keyword
                weather_match = find_keyword(weather_season_words, body)
                gear_match = find_keyword(hiking_gear_categories, body)
                if not (weather_match and gear_match):
                    continue

                # Look for brand mentions and run sentiment
                for match in brand_re.finditer(norm):
                    ctx = extract_context_window(norm, match, window_words=window_words)
                    sent_label, sent_score = get_sentiment(
                        ctx, threshold=sentiment_threshold, method=sentiment_method
                    )

                    data.append({
                        "subreddit": name,
                        "sub_name": submission.title,
                        "sub_link": submission.url,
                        "comment_created": pd.to_datetime(getattr(comment, "created_utc", None), unit="s", errors="coerce"),
                        "comment_body": body,
                        "weather_season": weather_match,
                        "gear_type": gear_match,
                        "gear_brand": match.group(0).lower(),
                        "brand_context": ctx,
                        "sent_label": str(sent_label),
                        "sent_score": float(sent_score) if sent_score is not None else None,
                    })

    return pd.DataFrame(data)
