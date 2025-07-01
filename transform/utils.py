# Databricks notebook source

# Property type for filtering (1 = houses)
property_type = 1

# Energy class scoring mapping
ENERGY_CLASS_SCORES = {
    'a': 10.0, 'A': 10.0,
    'b': 8.0,  'B': 8.0,
    'c': 6.0,  'C': 6.0,
    'd': 4.0,  'D': 4.0,
    'e': 2.0,  'E': 2.0,
    'f': 0.0,  'F': 0.0,
    'g': 0.0,  'G': 0.0,
    '': 3.0,   # Default for missing energy class
    None: 3.0
}

# Train stations and light rail stops accessible by bike from target zip codes
TRAIN_STATIONS = [
    {"name": "Aarhus H", "lat": 56.1496, "lon": 10.2045},
    {"name": "Skanderborg St", "lat": 55.9384, "lon": 9.9316},
    {"name": "Randers St", "lat": 56.4608, "lon": 10.0364},
    {"name": "Hadsten St", "lat": 56.3259, "lon": 10.0449},
    {"name": "Hinnerup St", "lat": 56.2827, "lon": 10.0419},
    {"name": "Langå St", "lat": 56.3889, "lon": 9.9028},
    
    # Letbane stops (Light Rail)
    {"name": "Risskov (Letbane)", "lat": 56.1836, "lon": 10.2238},
    {"name": "Skejby (Letbane)", "lat": 56.1927, "lon": 10.1722},
    {"name": "Universitetshospitalet (Letbane)", "lat": 56.1988, "lon": 10.1842},
    {"name": "Skejby Sygehus (Letbane)", "lat": 56.2033, "lon": 10.1742},
    {"name": "Lisbjerg Skole (Letbane)", "lat": 56.2178, "lon": 10.1662},
    {"name": "Lisbjerg Kirkeby (Letbane)", "lat": 56.2267, "lon": 10.1602},
    {"name": "Lystrup (Letbane)", "lat": 56.2356, "lon": 10.1542},
    {"name": "Ryomgård (Letbane)", "lat": 56.3792, "lon": 10.4928},
    {"name": "Grenaa (Letbane)", "lat": 56.4158, "lon": 10.8767}
]

MAX_DISTANCE_KM = 25.0

# Zip codes dictionary for validation
zipcodes_dict = {
    8000: "Århus C",
    8200: "Århus N",
    8210: "Århus V",
    8220: "Brabrand",
    8230: "Åbyhøj",
    8240: "Risskov",
    8250: "Egå",
    8260: "Viby J",
    8270: "Højbjerg",
    8300: "Odder",
    8310: "Tranbjerg J",
    8320: "Mårslet",
    8330: "Beder",
    8340: "Malling",
    8350: "Hundslund",
    8355: "Solbjerg",
    8361: "Hasselager",
    8362: "Hørning",
    8370: "Hadsten",
    8380: "Trige",
    8381: "Tilst",
    8382: "Hinnerup",
    8400: "Ebeltoft",
    8410: "Rønde",
    8420: "Knebel",
    8444: "Balle",
    8450: "Hammel",
    8462: "Harlev J",
    8464: "Galten",
    8471: "Sabro",
    8520: "Lystrup",
    8530: "Hjortshøj",
    8541: "Skødstrup",
    8543: "Hornslet",
    8550: "Ryomgård",
    8600: "Silkeborg",
    8660: "Skanderborg",
    8680: "Ry",
    8850: "Bjerringbro",
    8870: "Langå",
    8900: "Randers"
}
