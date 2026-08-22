#ETA configurations 
KENYA_LOCATIONS = {
    # Major Cities
    "NBO": {"city": "Nairobi", "base_time": 2.0, "zone": "urban"},
    "MBS": {"city": "Mombasa", "base_time": 3.0, "zone": "coastal"},
    "KIS": {"city": "Kisumu", "base_time": 4.0, "zone": "lake"},
    "NAK": {"city": "Nakuru", "base_time": 3.5, "zone": "highland"},
    "ELD": {"city": "Eldoret", "base_time": 4.5, "zone": "highland"},
    "THK": {"city": "Thika", "base_time": 2.5, "zone": "urban"},
    "MLD": {"city": "Malindi", "base_time": 4.0, "zone": "coastal"},
    "KTL": {"city": "Kitale", "base_time": 5.0, "zone": "highland"},
    "GSS": {"city": "Garissa", "base_time": 6.0, "zone": "arid"},
    "MER": {"city": "Meru", "base_time": 5.0, "zone": "highland"},
    "NYR": {"city": "Nyeri", "base_time": 3.5, "zone": "highland"},
    "EMB": {"city": "Embu", "base_time": 4.0, "zone": "highland"},
    "KAK": {"city": "Kakamega", "base_time": 5.0, "zone": "lake"},
    "BUN": {"city": "Bungoma", "base_time": 5.0, "zone": "lake"},
    "HOM": {"city": "Homa Bay", "base_time": 5.5, "zone": "lake"},
    "MIG": {"city": "Migori", "base_time": 5.5, "zone": "lake"},
    "BUS": {"city": "Busia", "base_time": 5.5, "zone": "lake"},
    "VHI": {"city": "Vihiga", "base_time": 5.0, "zone": "lake"},
    "SIA": {"city": "Siaya", "base_time": 5.0, "zone": "lake"},
    "KSM": {"city": "Kisii", "base_time": 5.0, "zone": "highland"},
    "NYA": {"city": "Nyamira", "base_time": 5.0, "zone": "highland"},
    "TAV": {"city": "Taveta", "base_time": 4.5, "zone": "coastal"},
    "WAT": {"city": "Watamu", "base_time": 4.0, "zone": "coastal"},
    "LAM": {"city": "Lamu", "base_time": 5.0, "zone": "coastal"},
    "MRB": {"city": "Maralal", "base_time": 6.0, "zone": "arid"},
    "LOD": {"city": "Lodwar", "base_time": 7.0, "zone": "arid"},
    "MARS": {"city": "Marsabit", "base_time": 8.0, "zone": "arid"},
    "MAND": {"city": "Mandera", "base_time": 8.0, "zone": "arid"},
    "WAJ": {"city": "Wajir", "base_time": 7.0, "zone": "arid"},
    "MSE": {"city": "Moyale", "base_time": 8.0, "zone": "arid"},
}

#Distance matrix
KENYA_DISTANCE_MATRIX = {
    "NBO": {"MBS": 8.0, "KIS": 6.0, "NAK": 3.0, "ELD": 5.0},
    "MBS": {"NBO": 8.0, "KIS": 10.0, "NAK": 9.0, "ELD": 11.0},
    "KIS": {"NBO": 6.0, "MBS": 10.0, "NAK": 5.0, "ELD": 3.0},
    "NAK": {"NBO": 3.0, "MBS": 9.0, "KIS": 5.0, "ELD": 2.0},
    "ELD": {"NBO": 5.0, "MBS": 11.0, "KIS": 3.0, "NAK": 2.0},
}

#Location pattern 
LOCATION_PATTERNS = {
    "WH": {"type": "warehouse", "processing_time": 2.0},  # hours
    "HUB": {"type": "hub", "processing_time": 1.5},
    "TRUCK": {"type": "in_transit", "processing_time": 0.5},
    "ZONE": {"type": "delivery", "processing_time": 0.5},
}

#state specific time 
STATE_TIME_MULTIPLIERS = {
    "picked": 1.0,
    "shipped": 1.2,
    "in_transit": 1.0,
    "out_for_delivery": 0.8,
    "delivered": 0.0,
}

#zone specific time adjustements 
ZONE_TIME_ADJUSTMENTS = {
    "urban": 1.0,      # Normal
    "highland": 1.3,   # 30% slower due to terrain
    "coastal": 1.2,    # 20% slower due to coastal traffic
    "lake": 1.15,      # 15% slower around lake region
    "arid": 1.5,       # 50% slower in arid areas
}

#peak hours 
PEAK_HOURS = {
    "morning": (7, 10),   # 7-10 AM
    "evening": (16, 19),  # 4-7 PM
}