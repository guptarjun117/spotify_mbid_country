import warnings

import pandas as pd
import folium
from folium.plugins import MarkerCluster, Fullscreen
import numpy as np

warnings.filterwarnings("ignore")

ISO_TO_COUNTRY = {
    "AF": "Afghanistan",
    "AX": "Åland Islands",
    "AL": "Albania",
    "DZ": "Algeria",
    "AS": "American Samoa",
    "AD": "Andorra",
    "AO": "Angola",
    "AI": "Anguilla",
    "AQ": "Antarctica",
    "AG": "Antigua and Barbuda",
    "AR": "Argentina",
    "AM": "Armenia",
    "AW": "Aruba",
    "AU": "Australia",
    "AT": "Austria",
    "AZ": "Azerbaijan",
    "BS": "Bahamas",
    "BH": "Bahrain",
    "BD": "Bangladesh",
    "BB": "Barbados",
    "BY": "Belarus",
    "BE": "Belgium",
    "BZ": "Belize",
    "BJ": "Benin",
    "BM": "Bermuda",
    "BT": "Bhutan",
    "BO": "Bolivia",
    "BQ": "Bonaire, Sint Eustatius and Saba",
    "BA": "Bosnia and Herzegovina",
    "BW": "Botswana",
    "BV": "Bouvet Island",
    "BR": "Brazil",
    "IO": "British Indian Ocean Territory",
    "VG": "British Virgin Islands",
    "BN": "Brunei",
    "BG": "Bulgaria",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "KH": "Cambodia",
    "CM": "Cameroon",
    "CA": "Canada",
    "CV": "Cape Verde",
    "KY": "Cayman Islands",
    "CF": "Central African Republic",
    "TD": "Chad",
    "CL": "Chile",
    "CN": "China",
    "CX": "Christmas Island",
    "CC": "Cocos (Keeling) Islands",
    "CO": "Colombia",
    "KM": "Comoros",
    "CG": "Congo",
    "CK": "Cook Islands",
    "CR": "Costa Rica",
    "CI": "Côte d'Ivoire",
    "HR": "Croatia",
    "CU": "Cuba",
    "CW": "Curaçao",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "XC": "Czechoslovakia",
    "CD": "Democratic Republic of the Congo",
    "DK": "Denmark",
    "DJ": "Djibouti",
    "DM": "Dominica",
    "DO": "Dominican Republic",
    "XG": "East Germany",
    "EC": "Ecuador",
    "EG": "Egypt",
    "SV": "El Salvador",
    "GQ": "Equatorial Guinea",
    "ER": "Eritrea",
    "EE": "Estonia",
    "SZ": "Eswatini",
    "ET": "Ethiopia",
    "XE": "Europe",
    "FK": "Falkland Islands",
    "FO": "Faroe Islands",
    "FM": "Federated States of Micronesia",
    "FJ": "Fiji",
    "FI": "Finland",
    "FR": "France",
    "GF": "French Guiana",
    "PF": "French Polynesia",
    "TF": "French Southern Territories",
    "GA": "Gabon",
    "GM": "Gambia",
    "GE": "Georgia",
    "DE": "Germany",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GR": "Greece",
    "GL": "Greenland",
    "GD": "Grenada",
    "GP": "Guadeloupe",
    "GU": "Guam",
    "GT": "Guatemala",
    "GG": "Guernsey",
    "GN": "Guinea",
    "GW": "Guinea-Bissau",
    "GY": "Guyana",
    "HT": "Haiti",
    "HM": "Heard Island and McDonald Islands",
    "HN": "Honduras",
    "HK": "Hong Kong",
    "HU": "Hungary",
    "IS": "Iceland",
    "IN": "India",
    "ID": "Indonesia",
    "IR": "Iran",
    "IQ": "Iraq",
    "IE": "Ireland",
    "IM": "Isle of Man",
    "IL": "Israel",
    "IT": "Italy",
    "JM": "Jamaica",
    "JP": "Japan",
    "JE": "Jersey",
    "JO": "Jordan",
    "KZ": "Kazakhstan",
    "KE": "Kenya",
    "KI": "Kiribati",
    "XK": "Kosovo",
    "KW": "Kuwait",
    "KG": "Kyrgyzstan",
    "LA": "Laos",
    "LV": "Latvia",
    "LB": "Lebanon",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libya",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MO": "Macao",
    "MG": "Madagascar",
    "MW": "Malawi",
    "MY": "Malaysia",
    "MV": "Maldives",
    "ML": "Mali",
    "MT": "Malta",
    "MH": "Marshall Islands",
    "MQ": "Martinique",
    "MR": "Mauritania",
    "MU": "Mauritius",
    "YT": "Mayotte",
    "MX": "Mexico",
    "MD": "Moldova",
    "MC": "Monaco",
    "MN": "Mongolia",
    "ME": "Montenegro",
    "MS": "Montserrat",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "MM": "Myanmar",
    "NA": "Namibia",
    "NR": "Nauru",
    "NP": "Nepal",
    "NL": "Netherlands",
    "AN": "Netherlands Antilles",
    "NC": "New Caledonia",
    "NZ": "New Zealand",
    "NI": "Nicaragua",
    "NE": "Niger",
    "NG": "Nigeria",
    "NU": "Niue",
    "NF": "Norfolk Island",
    "MP": "Northern Mariana Islands",
    "KP": "North Korea",
    "MK": "North Macedonia",
    "NO": "Norway",
    "OM": "Oman",
    "PK": "Pakistan",
    "PW": "Palau",
    "PS": "Palestine",
    "PA": "Panama",
    "PG": "Papua New Guinea",
    "PY": "Paraguay",
    "PE": "Peru",
    "PH": "Philippines",
    "PN": "Pitcairn",
    "PL": "Poland",
    "PT": "Portugal",
    "PR": "Puerto Rico",
    "QA": "Qatar",
    "RE": "Réunion",
    "RO": "Romania",
    "RU": "Russia",
    "RW": "Rwanda",
    "BL": "Saint Barthélemy",
    "SH": "Saint Helena, Ascension and Tristan da Cunha",
    "KN": "Saint Kitts and Nevis",
    "LC": "Saint Lucia",
    "MF": "Saint Martin (French part)",
    "PM": "Saint Pierre and Miquelon",
    "VC": "Saint Vincent and The Grenadines",
    "WS": "Samoa",
    "SM": "San Marino",
    "ST": "Sao Tome and Principe",
    "SA": "Saudi Arabia",
    "SN": "Senegal",
    "RS": "Serbia",
    "CS": "Serbia and Montenegro",
    "SC": "Seychelles",
    "SL": "Sierra Leone",
    "SG": "Singapore",
    "SX": "Sint Maarten (Dutch part)",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "SB": "Solomon Islands",
    "SO": "Somalia",
    "ZA": "South Africa",
    "GS": "South Georgia and the South Sandwich Islands",
    "KR": "South Korea",
    "SS": "South Sudan",
    "SU": "Soviet Union",
    "ES": "Spain",
    "LK": "Sri Lanka",
    "SD": "Sudan",
    "SR": "Suriname",
    "SJ": "Svalbard and Jan Mayen",
    "SE": "Sweden",
    "CH": "Switzerland",
    "SY": "Syria",
    "TW": "Taiwan",
    "TJ": "Tajikistan",
    "TZ": "Tanzania",
    "TH": "Thailand",
    "TL": "Timor-Leste",
    "TG": "Togo",
    "TK": "Tokelau",
    "TO": "Tonga",
    "TT": "Trinidad and Tobago",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TM": "Turkmenistan",
    "TC": "Turks and Caicos Islands",
    "TV": "Tuvalu",
    "UG": "Uganda",
    "UA": "Ukraine",
    "AE": "United Arab Emirates",
    "GB": "United Kingdom",
    "US": "United States",
    "UM": "United States Minor Outlying Islands",
    "UY": "Uruguay",
    "VI": "U.S. Virgin Islands",
    "UZ": "Uzbekistan",
    "VU": "Vanuatu",
    "VA": "Vatican City",
    "VE": "Venezuela",
    "VN": "Vietnam",
    "WF": "Wallis and Futuna",
    "EH": "Western Sahara",
    "XW": "[Worldwide]",
    "YE": "Yemen",
    "YU": "Yugoslavia",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}

def get_country_name(iso_code):
    """Convert ISO country code to full country name using our mapping."""
    if pd.isna(iso_code) or iso_code == "":
        return None
    return ISO_TO_COUNTRY.get(str(iso_code).strip(), iso_code)


def should_be_on_map(country_name):
    """Check if a country should appear on the map."""
    if not country_name:
        return False

    non_map_codes = ["XE", "XW", "XC", "XG", "XK", "SU", "YU", "CS", "AN", "XC", "XW"]

    iso_code = None
    for code, name in ISO_TO_COUNTRY.items():
        if name == country_name:
            iso_code = code
            break

    if iso_code and iso_code in non_map_codes:
        return False

    return True

COUNTRY_COORDS = {
    "Afghanistan": [33.9391, 67.71],
    "Åland Islands": [60.1167, 19.9],
    "Albania": [41.1533, 20.1683],
    "Algeria": [28.0339, 1.6596],
    "American Samoa": [-14.271, -170.1322],
    "Andorra": [42.5063, 1.5218],
    "Angola": [-11.2027, 17.8739],
    "Anguilla": [18.2206, -63.0686],
    "Antarctica": [-75.251, -0.0714],
    "Antigua and Barbuda": [17.0608, -61.7964],
    "Argentina": [-38.4161, -63.6167],
    "Armenia": [40.0691, 45.0382],
    "Aruba": [12.5211, -69.9683],
    "Australia": [-25.2744, 133.7751],
    "Austria": [47.5162, 14.5501],
    "Azerbaijan": [40.1431, 47.5769],
    "Bahamas": [25.0343, -77.3963],
    "Bahrain": [26.0667, 50.5577],
    "Bangladesh": [23.685, 90.3563],
    "Barbados": [13.1939, -59.5432],
    "Belarus": [53.7098, 27.9534],
    "Belgium": [50.5039, 4.4699],
    "Belize": [17.1899, -88.4976],
    "Benin": [9.3077, 2.3158],
    "Bermuda": [32.3078, -64.7505],
    "Bhutan": [27.5142, 90.4336],
    "Bolivia": [-16.2902, -63.5887],
    "Bonaire, Sint Eustatius and Saba": [12.1784, -68.2385],
    "Bosnia and Herzegovina": [43.9159, 17.6791],
    "Botswana": [-22.3285, 24.6849],
    "Bouvet Island": [-54.4232, 3.4132],
    "Brazil": [-10.0, -55.0],
    "British Indian Ocean Territory": [-6.3431, 71.8765],
    "British Virgin Islands": [18.4207, -64.64],
    "Brunei": [4.5353, 114.7277],
    "Bulgaria": [42.7339, 25.4858],
    "Burkina Faso": [12.2383, -1.5616],
    "Burundi": [-3.3731, 29.9189],
    "Cambodia": [12.5657, 104.991],
    "Cameroon": [7.3697, 12.3547],
    "Canada": [60.0, -95.0],
    "Cape Verde": [16.5388, -23.0418],
    "Cayman Islands": [19.3133, -81.2546],
    "Central African Republic": [6.6111, 20.9394],
    "Chad": [15.4542, 18.7322],
    "Chile": [-35.6751, -71.543],
    "China": [35.8617, 104.1954],
    "Christmas Island": [-10.4475, 105.6904],
    "Cocos (Keeling) Islands": [-12.1642, 96.871],
    "Colombia": [4.5709, -74.2973],
    "Comoros": [-11.6455, 43.3333],
    "Congo": [-0.228, 15.8277],
    "Cook Islands": [-21.2367, -159.7777],
    "Costa Rica": [9.7489, -83.7534],
    "Côte d'Ivoire": [7.54, -5.5471],
    "Croatia": [45.1, 15.2],
    "Cuba": [21.5218, -77.7812],
    "Curaçao": [12.1696, -68.99],
    "Cyprus": [35.1264, 33.4299],
    "Czechia": [49.8175, 15.473],
    "Czechoslovakia": [49.8175, 15.473],
    "Democratic Republic of the Congo": [-4.0383, 21.7587],
    "Denmark": [56.2639, 9.5018],
    "Djibouti": [11.8251, 42.5903],
    "Dominica": [15.415, -61.371],
    "Dominican Republic": [18.7357, -70.1627],
    "East Germany": [52.0, 12.0],
    "Ecuador": [-1.8312, -78.1834],
    "Egypt": [26.8206, 30.8025],
    "El Salvador": [13.7942, -88.8965],
    "Equatorial Guinea": [1.6508, 10.2679],
    "Eritrea": [15.1794, 39.7823],
    "Estonia": [58.5953, 25.0136],
    "Eswatini": [-26.5225, 31.4659],
    "Ethiopia": [9.145, 40.4897],
    "Europe": [54.526, 15.2551],
    "Falkland Islands": [-51.7963, -59.5236],
    "Faroe Islands": [61.8926, -6.9118],
    "Federated States of Micronesia": [6.9177, 158.1589],
    "Fiji": [-17.7134, 178.065],
    "Finland": [61.9241, 25.7482],
    "France": [46.2276, 2.2137],
    "French Guiana": [3.9339, -53.1258],
    "French Polynesia": [-17.6797, -149.4068],
    "French Southern Territories": [-49.2803, 69.3485],
    "Gabon": [-0.8037, 11.6094],
    "Gambia": [13.4432, -15.3101],
    "Georgia": [42.3154, 43.3569],
    "Germany": [51.1657, 10.4515],
    "Ghana": [7.9465, -1.0232],
    "Gibraltar": [36.1408, -5.3536],
    "Greece": [39.0742, 21.8243],
    "Greenland": [71.7069, -42.6043],
    "Grenada": [12.1165, -61.6791],
    "Guadeloupe": [16.265, -61.551],
    "Guam": [13.4443, 144.7937],
    "Guatemala": [15.7835, -90.2308],
    "Guernsey": [49.4657, -2.5853],
    "Guinea": [9.9456, -9.6966],
    "Guinea-Bissau": [11.8037, -15.1804],
    "Guyana": [4.8604, -58.9302],
    "Haiti": [18.9712, -72.2852],
    "Heard Island and McDonald Islands": [-53.0818, 73.5042],
    "Honduras": [15.2, -86.2419],
    "Hong Kong": [22.3964, 114.1095],
    "Hungary": [47.1625, 19.5033],
    "Iceland": [64.9631, -19.0208],
    "India": [20.5937, 78.9629],
    "Indonesia": [-0.7893, 113.9213],
    "Iran": [32.4279, 53.688],
    "Iraq": [33.2232, 43.6793],
    "Ireland": [53.4129, -8.2439],
    "Isle of Man": [54.2361, -4.5481],
    "Israel": [31.0461, 34.8516],
    "Italy": [41.8719, 12.5674],
    "Jamaica": [18.1096, -77.2975],
    "Japan": [36.2048, 138.2529],
    "Jersey": [49.2144, -2.1312],
    "Jordan": [30.5852, 36.2384],
    "Kazakhstan": [48.0196, 66.9237],
    "Kenya": [-0.0236, 37.9062],
    "Kiribati": [-3.3704, -168.734],
    "Kosovo": [42.6026, 20.903],
    "Kuwait": [29.3117, 47.4818],
    "Kyrgyzstan": [41.2044, 74.7661],
    "Laos": [19.8563, 102.4955],
    "Latvia": [56.8796, 24.6032],
    "Lebanon": [33.8547, 35.8623],
    "Lesotho": [-29.61, 28.2336],
    "Liberia": [6.4281, -9.4295],
    "Libya": [26.3351, 17.2283],
    "Liechtenstein": [47.166, 9.5554],
    "Lithuania": [55.1694, 23.8813],
    "Luxembourg": [49.8153, 6.1296],
    "Macao": [22.1987, 113.5439],
    "Madagascar": [-18.7669, 46.8691],
    "Malawi": [-13.2543, 34.3015],
    "Malaysia": [4.2105, 101.9758],
    "Maldives": [3.2028, 73.2207],
    "Mali": [17.5707, -3.9962],
    "Malta": [35.9375, 14.3754],
    "Marshall Islands": [7.1315, 171.1845],
    "Martinique": [14.6415, -61.0242],
    "Mauritania": [21.0079, -10.9408],
    "Mauritius": [-20.3484, 57.5522],
    "Mayotte": [-12.8275, 45.1662],
    "Mexico": [23.6345, -102.5528],
    "Moldova": [47.4116, 28.3699],
    "Monaco": [43.7384, 7.4246],
    "Mongolia": [46.8625, 103.8467],
    "Montenegro": [42.7087, 19.3744],
    "Montserrat": [16.7425, -62.1874],
    "Morocco": [31.7917, -7.0926],
    "Mozambique": [-18.6657, 35.5296],
    "Myanmar": [21.9162, 95.956],
    "Namibia": [-22.9576, 18.4904],
    "Nauru": [-0.5228, 166.9315],
    "Nepal": [28.3949, 84.124],
    "Netherlands": [52.1326, 5.2913],
    "Netherlands Antilles": [12.2261, -69.0601],
    "New Caledonia": [-20.9043, 165.618],
    "New Zealand": [-40.9006, 174.886],
    "Nicaragua": [12.8654, -85.2072],
    "Niger": [17.6078, 8.0817],
    "Nigeria": [9.082, 8.6753],
    "Niue": [-19.0544, -169.8672],
    "Norfolk Island": [-29.0408, 167.9547],
    "Northern Mariana Islands": [15.0979, 145.6739],
    "North Korea": [40.3399, 127.5101],
    "North Macedonia": [41.6086, 21.7453],
    "Norway": [60.472, 8.4689],
    "Oman": [21.5126, 55.9233],
    "Pakistan": [30.3753, 69.3451],
    "Palau": [7.515, 134.5825],
    "Palestine": [31.9522, 35.2332],
    "Panama": [8.538, -80.7821],
    "Papua New Guinea": [-6.315, 143.9555],
    "Paraguay": [-23.4425, -58.4438],
    "Peru": [-9.19, -75.0152],
    "Philippines": [12.8797, 121.774],
    "Pitcairn": [-24.7036, -127.4393],
    "Poland": [51.9194, 19.1451],
    "Portugal": [39.3999, -8.2245],
    "Puerto Rico": [18.2208, -66.5901],
    "Qatar": [25.3548, 51.1839],
    "Réunion": [-21.1151, 55.5364],
    "Romania": [45.9432, 24.9668],
    "Russia": [61.524, 105.3188],
    "Rwanda": [-1.9403, 29.8739],
    "Saint Barthélemy": [17.9, -62.8333],
    "Saint Helena, Ascension and Tristan da Cunha": [-15.965, -5.7089],
    "Saint Kitts and Nevis": [17.3578, -62.783],
    "Saint Lucia": [13.9094, -60.9789],
    "Saint Martin (French part)": [18.0708, -63.05],
    "Saint Pierre and Miquelon": [46.8852, -56.3159],
    "Saint Vincent and The Grenadines": [12.9843, -61.2872],
    "Samoa": [-13.759, -172.1046],
    "San Marino": [43.9424, 12.4578],
    "Sao Tome and Principe": [0.1864, 6.6131],
    "Saudi Arabia": [23.8859, 45.0792],
    "Senegal": [14.4974, -14.4524],
    "Serbia": [44.0165, 21.0059],
    "Serbia and Montenegro": [44.0165, 21.0059],
    "Seychelles": [-4.6796, 55.492],
    "Sierra Leone": [8.4606, -11.7799],
    "Singapore": [1.3521, 103.8198],
    "Sint Maarten (Dutch part)": [18.0425, -63.0548],
    "Slovakia": [48.669, 19.699],
    "Slovenia": [46.1512, 14.9955],
    "Solomon Islands": [-9.6457, 160.1562],
    "Somalia": [5.1521, 46.1996],
    "South Africa": [-30.5595, 22.9375],
    "South Georgia and the South Sandwich Islands": [-54.4296, -36.5879],
    "South Korea": [35.9078, 127.7669],
    "South Sudan": [6.877, 31.307],
    "Soviet Union": [60.0, 90.0],
    "Spain": [40.4637, -3.7492],
    "Sri Lanka": [7.8731, 80.7718],
    "Sudan": [12.8628, 30.2176],
    "Suriname": [3.9193, -56.0278],
    "Svalbard and Jan Mayen": [77.5536, 23.6703],
    "Sweden": [62.0, 15.0],
    "Switzerland": [46.8182, 8.2275],
    "Syria": [34.8021, 38.9968],
    "Taiwan": [23.6978, 120.9605],
    "Tajikistan": [38.861, 71.2761],
    "Tanzania": [-6.369, 34.8888],
    "Thailand": [15.87, 100.9925],
    "Timor-Leste": [-8.8742, 125.7275],
    "Togo": [8.6195, 0.8248],
    "Tokelau": [-9.2002, -171.8484],
    "Tonga": [-21.179, -175.1982],
    "Trinidad and Tobago": [10.6918, -61.2225],
    "Tunisia": [33.8869, 9.5375],
    "Turkey": [38.9637, 35.2433],
    "Turkmenistan": [38.9697, 59.5563],
    "Turks and Caicos Islands": [21.694, -71.7979],
    "Tuvalu": [-7.1095, 177.6493],
    "Uganda": [1.3733, 32.2903],
    "Ukraine": [48.3794, 31.1656],
    "United Arab Emirates": [23.4241, 53.8478],
    "United Kingdom": [55.3781, -3.436],
    "United States": [37.0902, -95.7129],
    "United States Minor Outlying Islands": [19.2833, 166.6],
    "Uruguay": [-32.5228, -55.7658],
    "U.S. Virgin Islands": [18.3358, -64.8963],
    "Uzbekistan": [41.3775, 64.5853],
    "Vanuatu": [-15.3767, 166.9592],
    "Vatican City": [41.9029, 12.4534],
    "Venezuela": [6.4238, -66.5897],
    "Vietnam": [14.0583, 108.2772],
    "Wallis and Futuna": [-13.2944, -176.2044],
    "Western Sahara": [24.2155, -12.8858],
    "[Worldwide]": [0.0, 0.0],
    "Yemen": [15.5527, 48.5164],
    "Yugoslavia": [44.0165, 21.0059],
    "Zambia": [-13.1339, 27.8493],
    "Zimbabwe": [-19.0154, 29.1549],
}

def build_map(csv_path, output_html_path):
    df = pd.read_csv(csv_path)

    df_all = df.copy()

    df_all["country_name"] = df_all["country"].apply(get_country_name)

    df_valid = df_all[df_all["country_name"].notna() & df_all["country_name"].apply(should_be_on_map)].copy()

    non_map_codes = ["XE", "XW", "XC", "XG", "XK", "SU", "YU", "CS", "AN"]
    df_non_map = df_all[df_all["country"].isin(non_map_codes)].copy()
    df_non_map["country_name"] = df_non_map["country"].apply(get_country_name)

    df_no_country = df_all[df_all["country_name"].isna() | (df_all["country"] == "")].copy()

    country_counts = df_valid.groupby("country_name").size().reset_index(name="artist_count")
    country_counts = country_counts.sort_values("artist_count", ascending=False)

    non_map_counts = df_non_map.groupby("country_name").size().reset_index(name="artist_count")
    non_map_counts = non_map_counts.sort_values("artist_count", ascending=False)

    m = folium.Map(
        location=[30, 0],
        zoom_start=2,
        tiles="cartodbdark_matter",
        control_scale=True,
        prefer_canvas=True,
    )

    marker_cluster = MarkerCluster(
        name="Artists by Country",
        options={"showCoverageOnHover": False, "zoomToBoundsOnClick": True},
    ).add_to(m)

    artist_counts = country_counts["artist_count"].values
    p80 = np.percentile(artist_counts, 80)
    p60 = np.percentile(artist_counts, 60)
    p40 = np.percentile(artist_counts, 40)
    p20 = np.percentile(artist_counts, 20)

    actual_p80 = int(np.ceil(p80)) if not np.isnan(p80) else 0
    actual_p60 = int(np.ceil(p60)) if not np.isnan(p60) else 0
    actual_p40 = int(np.ceil(p40)) if not np.isnan(p40) else 0
    actual_p20 = int(np.ceil(p20)) if not np.isnan(p20) else 0

    def get_bin_color(count, p20, p40, p60, p80):
        """Get color based on percentile bins."""
        if count >= p80:
            return "#1565C0"  # Top 20% - darkest blue
        if count >= p60:
            return "#4a7bb9"  # 60-80%
        if count >= p40:
            return "#7a9bc6"  # 40-60%
        if count >= p20:
            return "#b92b27"  # 20-40%
        return "#b92b27"  # Bottom 20% - red

    for _, row in country_counts.iterrows():
        country = row["country_name"]
        count = row["artist_count"]
        coords = COUNTRY_COORDS.get(country, [20, 0])

        country_artists = df_valid[df_valid["country_name"] == country]["artist_name"].tolist()

        popup_content = f"""
    <div class="popup-wrapper" style="font-family: Arial, sans-serif; width: 350px; max-height: 400px;">
        <div class="popup-header" style="background: linear-gradient(135deg, #0F2027 0%, #2C5364 100%);
                    color: white; padding: 15px; border-radius: 5px 5px 0 0;">
            <h3 class="popup-title" style="margin: 0; font-size: 18px;">{country}</h3>
            <div class="popup-badges" style="display: flex; justify-content: space-between; margin-top: 8px;">
                <span class="popup-badge" style="background: rgba(255,255,255,0.2); padding: 3px 8px; border-radius: 12px;">
                    {count} Artist{'s' if count != 1 else ''}/Band{'s' if count != 1 else ''}
                </span>
                <span class="popup-badge" style="background: rgba(255,255,255,0.2); padding: 3px 8px; border-radius: 12px;">
                    {df_valid[df_valid['country_name'] == country]['country'].iloc[0]}
                </span>
            </div>
        </div>

        <div class="popup-body" style="padding: 15px; max-height: 250px; overflow-y: auto;">
            <div class="popup-list" style="display: grid; grid-template-columns: 1fr; gap: 5px;">
    """

        for artist in country_artists:
            popup_content += (
                '<div class="popup-item" style="padding: 5px; border-left: 3px solid #764ba2; '
                f'background: #f9f9f9;">• {artist}</div>'
            )

        popup_content += """
            </div>
        </div>

        <div class="popup-footer" style="background: #f5f5f5; padding: 10px; border-radius: 0 0 5px 5px; border-top: 1px solid #ddd;">
            <div class="popup-footer-text" style="font-size: 12px; color: #666; text-align: center;">
                Click outside to close • Scroll to see all artists
            </div>
        </div>
    </div>
    """

        color = get_bin_color(count, p20, p40, p60, p80)

        min_count = country_counts["artist_count"].min()
        max_count = country_counts["artist_count"].max()

        if max_count > min_count:
            log_count = np.log1p(count)
            log_min = np.log1p(min_count)
            log_max = np.log1p(max_count)

            if log_max > log_min:
                scale_factor = (log_count - log_min) / (log_max - log_min)
                radius = 6 + (scale_factor * 12)
            else:
                radius = 10
        else:
            radius = 10

        folium.CircleMarker(
            location=coords,
            radius=radius,
            popup=folium.Popup(popup_content, max_width=400),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            weight=2,
            tooltip=f"<b>{country}</b><br>{count} artist{'s' if count != 1 else ''}",
            name=country,
        ).add_to(marker_cluster)

    bin_20plus = len(country_counts[country_counts["artist_count"] >= p80])
    bin_60to80 = len(country_counts[(country_counts["artist_count"] >= p60) & (country_counts["artist_count"] < p80)])
    bin_40to60 = len(country_counts[(country_counts["artist_count"] >= p40) & (country_counts["artist_count"] < p60)])
    bin_20to40 = len(country_counts[(country_counts["artist_count"] >= p20) & (country_counts["artist_count"] < p40)])
    bin_0to20 = len(country_counts[country_counts["artist_count"] < p20])

    non_map_artists_list = []
    for _, row in df_non_map.iterrows():
        country_name = row["country_name"]
        artist_name = row["artist_name"]
        non_map_artists_list.append(f"{artist_name} ({country_name})")

    no_country_artists_list = df_no_country["artist_name"].tolist()

    total_artists = len(df_all)
    valid_countries = len(country_counts)
    no_country_count = len(df_no_country)
    non_map_count = len(df_non_map)

    stats_html = f"""
<div id="stats-sidebar" style="
    position: absolute;
    top: 60px;
    right: 10px;
    width: 300px;
    background: rgba(45, 45, 45, 0.95);
    border-radius: 8px;
    padding: 12px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    z-index: 1000;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    max-height: calc(95vh - 70px);
    overflow-y: auto;
    color: #e0e0e0;
    border: 1px solid #444;
">
    <div style="
        background: linear-gradient(135deg, #0F2027 0%, #2C5364 100%);
        color: white;
        padding: 12px;
        border-radius: 6px;
        margin-bottom: 12px;
    ">
        <h2 style="margin: 0 0 3px 0; font-size: 18px; font-weight: 600;">Spotify Playlist Geo-Analyzer</h2>
        <div style="font-size: 11px; opacity: 0.9; display: flex; justify-content: space-between;">
            <span> Interactive Map • Click markers for Artist/Band lists </span>
        </div>
    </div>

    <div style="margin-bottom: 16px;">
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 8px;">
            <div style="background: rgba(255,255,255,0.05); padding: 8px; border-radius: 6px; text-align: center; border: 1px solid #444;">
                <div style="font-size: 20px; font-weight: 700; color: #e6e6e6;">{total_artists}</div>
                <div style="font-size: 10px; color: #aaa; font-weight: 500;">Total Artists</div>
            </div>
            <div style="background: rgba(255,255,255,0.05); padding: 8px; border-radius: 6px; text-align: center; border: 1px solid #444;">
                <div style="font-size: 20px; font-weight: 700; color: #e6e6e6;">{valid_countries}</div>
                <div style="font-size: 10px; color: #aaa; font-weight: 500;">Countries</div>
            </div>
        </div>
    </div>

    <div style="margin-bottom: 16px;">
        <div style="display: flex; align-items: center; margin-bottom: 8px;">
            <h3 style="margin: 0; color: #fff; font-size: 14px; font-weight: 600;">All Countries</h3>
        </div>
        <div style="max-height: 180px; overflow-y: auto;">
            <div style="font-size: 11px; color: #aaa; margin-bottom: 6px; padding: 0 2px;">
                <div style="display: flex; justify-content: space-between;">
                    <span>Country</span>
                    <span>Artists</span>
                </div>
            </div>
            <div style="border-top: 1px solid #444;">
"""

    for i, (_, row) in enumerate(country_counts.iterrows()):
        country = row["country_name"]
        count = row["artist_count"]

        display_name = country[:18] + "..." if len(country) > 18 else country

        stats_html += f"""
                <div style="padding: 6px 2px; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-size: 12px; color: #e0e0e0; font-weight: {'600' if i < 3 else '400'}">{display_name}</span>
                    <span style="font-size: 12px; font-weight: 600; color: #e6e6e6;">{count}</span>
                </div>
"""

    stats_html += f"""
            </div>
        </div>
    </div>

    <div style="margin-bottom: 12px;">
        <div style="display: flex; align-items: center; margin-bottom: 8px;">
            <h3 style="margin: 0; color: #fff; font-size: 14px; font-weight: 600;">Missing Locations</h3>
        </div>

        <div style="background: rgba(156, 163, 175, 0.1); border-radius: 6px; border: 1px solid #9ca3af; margin-bottom: 8px;">
            <div style="padding: 10px; border-bottom: 1px solid #444;">
                <div style="display: flex; align-items: center; justify-content: space-between;">
                    <div>
                        <div style="font-size: 16px; font-weight: 700; color: #9ca3af;">{non_map_count}</div>
                        <div style="font-size: 10px; color: #d1d5db; font-weight: 500;">Non-specific regions</div>
                    </div>
                </div>
            </div>
            <div style="font-size: 11px; color: #d1d5db; max-height: 120px; overflow-y: auto; padding: 10px;">
"""

    if non_map_artists_list:
        for artist in non_map_artists_list:
            country_code = artist.split("(")[-1].replace(")", "") if "(" in artist else ""
            artist_name_only = artist.split("(")[0].strip() if "(" in artist else artist
            stats_html += (
                '<div style="margin: 4px 0; padding: 3px 0; '
                'border-bottom: 1px solid rgba(252,165,165,0.2);">• '
                f"{artist_name_only} <span style=\"color: #9ca3af; font-size: 10px;\">({country_code})</span></div>"
            )
    else:
        stats_html += (
            '<div style="color: #999; font-size: 10px; text-align: center; padding: 10px;">'
            "No artists in this category</div>"
        )

    stats_html += f"""
            </div>
        </div>

        <div style="background: rgba(156, 163, 175, 0.1); border-radius: 6px; border: 1px solid #9ca3af;">
            <div style="padding: 10px; border-bottom: 1px solid #444;">
                <div style="display: flex; align-items: center; justify-content: space-between;">
                    <div>
                        <div style="font-size: 16px; font-weight: 700; color: #9ca3af;">{no_country_count}</div>
                        <div style="font-size: 10px; color: #d1d5db; font-weight: 500;">No country data in MusicBrainz Database</div>
                    </div>
                </div>
            </div>
            <div style="font-size: 11px; color: #d1d5db; max-height: 120px; overflow-y: auto; padding: 10px;">
"""
    if no_country_artists_list:
        for artist in no_country_artists_list:
            stats_html += (
                '<div style="margin: 4px 0; padding: 3px 0; '
                f'border-bottom: 1px solid rgba(209,213,219,0.2);">• {artist}</div>'
            )
    else:
        stats_html += (
            '<div style="color: #999; font-size: 10px; text-align: center; padding: 10px;">'
            "No artists in this category</div>"
        )

    stats_html += """
            </div>
        </div>
    </div>

    <div style="font-size: 10px; color: #9ca3af; text-align: center; padding-top: 8px; border-top: 1px solid #444;">
    </div>
</div>
"""

    bins_html = f"""
<div id="bins-panel" style="
    position: absolute;
    bottom: 90px;
    left: 10px;
    width: 220px;
    background: rgba(45, 45, 45, 0.95);
    border-radius: 8px;
    padding: 10px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    z-index: 1000;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    font-size: 11px;
    color: #e0e0e0;
    border: 1px solid #444;
">
    <div style="display: flex; align-items: center; margin-bottom: 8px;">
        <h4 style="margin: 0; color: #fff; font-size: 12px; font-weight: 600;">Artist Count Bins</h4>
    </div>

    <div style="margin-bottom: 10px; padding: 5px; background: rgba(255,255,255,0.05); border-radius: 4px;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 4px; font-size: 9px;">
            <span style="color: #d1d5db;">Low ({min(artist_counts)})</span>
            <span style="color: #d1d5db;">High ({max(artist_counts)})</span>
        </div>
        <div style="
            height: 8px;
            border-radius: 4px;
            background: linear-gradient(90deg, #b92b27 0%, #1565C0 100%);
            margin-bottom: 4px;
        "></div>
        <div style="font-size: 9px; color: #aaa; text-align: center;">
        </div>
    </div>

    <div style="display: grid; grid-template-columns: auto 1fr auto; gap: 6px; align-items: center; font-size: 10px;">
        <div style="display: flex; align-items: center;">
            <div style="width: 10px; height: 10px; background: #1565C0; border-radius: 50%; margin-right: 6px;"></div>
            <span style="color: #ccc;">Top 20%</span>
        </div>
        <div style="text-align: center; color: #aaa; font-size: 9px;">
            ≥{actual_p80}
        </div>
        <div style="text-align: right; font-weight: 700; color: #ccc; font-size: 11px;">
            {bin_20plus}
        </div>

        <div style="display: flex; align-items: center;">
            <div style="width: 9px; height: 9px; background: #4a7bb9; border-radius: 50%; margin-right: 6px;"></div>
            <span style="color: #ccc;">60-80%</span>
        </div>
        <div style="text-align: center; color: #aaa; font-size: 9px;">
            {actual_p60}-{actual_p80-1}
        </div>
        <div style="text-align: right; font-weight: 700; color: #ccc; font-size: 11px;">
            {bin_60to80}
        </div>

        <div style="display: flex; align-items: center;">
            <div style="width: 8px; height: 8px; background: #7a9bc6; border-radius: 50%; margin-right: 6px;"></div>
            <span style="color: #ccc;">40-60%</span>
        </div>
        <div style="text-align: center; color: #aaa; font-size: 9px;">
            {actual_p40}-{actual_p60-1}
        </div>
        <div style="text-align: right; font-weight: 700; color: #ccc; font-size: 11px;">
            {bin_40to60}
        </div>

        <div style="display: flex; align-items: center;">
            <div style="width: 7px; height: 7px; background: #b92b27; border-radius: 50%; margin-right: 6px;"></div>
            <span style="color: #ccc;">20-40%</span>
        </div>
        <div style="text-align: center; color: #aaa; font-size: 9px;">
            {actual_p20}-{actual_p40-1}
        </div>
        <div style="text-align: right; font-weight: 700; color: #ccc; font-size: 11px;">
            {bin_20to40}
        </div>
    </div>

    <div style="margin-top: 10px; padding-top: 8px; border-top: 1px solid #444; font-size: 9px; color: #aaa;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 2px;">
            <span>Data points:</span>
            <span><strong>{total_artists}</strong></span>
        </div>
        <div style="display: flex; justify-content: space-between;">
            <span>On map:</span>
            <span><strong>{len(df_valid)}</strong> ({((len(df_valid)/total_artists)*100):.1f}%)</span>
        </div>
    </div>
</div>
"""

    m.get_root().html.add_child(folium.Element(stats_html))
    m.get_root().html.add_child(folium.Element(bins_html))

    toggle_controls = """
<button id="toggle-stats-btn" type="button" aria-label="Toggle dashboard">☰ Dashboard</button>
<button id="toggle-bins-btn" type="button" aria-label="Toggle bins">☰ Bins</button>

<script>
(function() {
  function isMobile() {
    return window.matchMedia("(max-width: 768px)").matches;
  }

  function setInitialState() {
    var stats = document.getElementById("stats-sidebar");
    var bins  = document.getElementById("bins-panel");
    if (!stats || !bins) return;

    // On mobile: start hidden. On desktop: start shown.
    if (isMobile()) {
      stats.classList.add("collapsed");
      bins.classList.add("collapsed");
      stats.classList.remove("open");
      bins.classList.remove("open");
    } else {
      stats.classList.remove("collapsed");
      bins.classList.remove("collapsed");
      stats.classList.remove("open");
      bins.classList.remove("open");
    }
  }

  function wireToggles() {
    var statsBtn = document.getElementById("toggle-stats-btn");
    var binsBtn  = document.getElementById("toggle-bins-btn");
    var stats    = document.getElementById("stats-sidebar");
    var bins     = document.getElementById("bins-panel");
    if (!statsBtn || !binsBtn || !stats || !bins) return;

    statsBtn.addEventListener("click", function(e) {
      e.preventDefault();
      stats.classList.toggle("collapsed");
      stats.classList.toggle("open");
    });

    binsBtn.addEventListener("click", function(e) {
      e.preventDefault();
      bins.classList.toggle("collapsed");
      bins.classList.toggle("open");
    });

    // Close drawers when tapping the map area on mobile
    document.addEventListener("click", function(e) {
      if (!isMobile()) return;

      var clickedInsideStats = stats.contains(e.target) || statsBtn.contains(e.target);
      var clickedInsideBins  = bins.contains(e.target)  || binsBtn.contains(e.target);

      if (!clickedInsideStats) {
        stats.classList.add("collapsed");
        stats.classList.remove("open");
      }
      if (!clickedInsideBins) {
        bins.classList.add("collapsed");
        bins.classList.remove("open");
      }
    }, true);

    // Keep behavior consistent on rotate / resize
    window.addEventListener("resize", function() {
      setInitialState();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function() {
      setInitialState();
      wireToggles();
    });
  } else {
    setInitialState();
    wireToggles();
  }
})();
</script>
"""
    m.get_root().html.add_child(folium.Element(toggle_controls))

    css = """
<style>
    @viewport {
        width: device-width;
        zoom: 1.0;
    }

    ::-webkit-scrollbar {
        width: 6px;
        height: 6px;
    }
    ::-webkit-scrollbar-track {
        background: rgba(255,255,255,0.05);
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb {
        background: #4f46e5;
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #7c3aed;
    }

    .leaflet-popup-content::-webkit-scrollbar {
        width: 6px;
    }
    .leaflet-popup-content::-webkit-scrollbar-track {
        background: #f8fafc;
    }
    .leaflet-popup-content::-webkit-scrollbar-thumb {
        background: #4f46e5;
    }

    .leaflet-container {
        background: #2d3748 !important;
    }

    .leaflet-popup-content-wrapper {
        padding: 0 !important;
        box-shadow: none !important;
        background: transparent !important;
        border: none !important;
    }

    .leaflet-popup-content {
        margin: 0 !important;
        padding: 0 !important;
    }

    .popup-wrapper {
        background: white !important;
        border-radius: 5px !important;
        box-shadow: 0 4px 20px rgba(0,0,0,0.15) !important;
        overflow: hidden !important;
    }

    .leaflet-popup-tip-container {
        display: none !important;
    }

    .leaflet-popup {
        margin-bottom: 0 !important;
        box-shadow: none !important;
    }

    .leaflet-popup-close-button {
        top: 8px !important;
        right: 8px !important;
        padding: 4px 8px !important;
        font-size: 20px !important;
        line-height: 1 !important;
        color: white !important;
        font-weight: 400 !important;
        text-shadow: none !important;
        background: transparent !important;
        border: none !important;
        z-index: 1001 !important;
        width: auto !important;
        height: auto !important;
    }

    .leaflet-popup-close-button:hover {
        color: #ccc !important;
        background: rgba(255,255,255,0.1) !important;
        border-radius: 4px !important;
    }

    .leaflet-popup-close-button:before,
    .leaflet-popup-close-button:after {
        display: none !important;
    }

    @media screen and (max-width: 768px) {
        .popup-wrapper { width: 280px !important; max-height: 350px !important; }
        .popup-header { padding: 12px !important; }
        .popup-title { font-size: 16px !important; }
        .popup-badge { font-size: 11px !important; }
        .popup-body { padding: 12px !important; max-height: 200px !important; }
        .popup-subtitle { font-size: 13px !important; }
        .popup-item { padding: 4px !important; font-size: 11px !important; }
        .popup-footer { padding: 8px !important; }
        .popup-footer-text { font-size: 10px !important; }
    }

    @media screen and (max-width: 480px) {
        .popup-wrapper { width: 240px !important; max-height: 300px !important; }
        .popup-header { padding: 10px !important; }
        .popup-title { font-size: 14px !important; }
        .popup-badges { flex-wrap: wrap !important; gap: 4px !important; }
        .popup-badge { font-size: 10px !important; padding: 2px 6px !important; }
        .popup-body { padding: 10px !important; max-height: 170px !important; }
        .popup-subtitle { font-size: 12px !important; }
        .popup-list { gap: 3px !important; }
        .popup-item { padding: 3px !important; font-size: 10px !important; }
        .popup-footer { padding: 6px !important; }
        .popup-footer-text { font-size: 9px !important; }
    }

    @media screen and (max-width: 360px) {
        .popup-wrapper { width: 200px !important; max-height: 280px !important; }
        .popup-header { padding: 8px !important; }
        .popup-title { font-size: 13px !important; }
        .popup-badge { font-size: 9px !important; padding: 2px 5px !important; }
        .popup-body { padding: 8px !important; max-height: 150px !important; }
        .popup-subtitle { font-size: 11px !important; }
        .popup-item { padding: 2px !important; font-size: 9px !important; }
        .popup-footer { padding: 5px !important; }
        .popup-footer-text { font-size: 8px !important; }
    }

    @media screen and (max-width: 1024px) {
        #stats-sidebar {
            width: 260px !important;
            font-size: 10px !important;
            top: 60px !important;
            max-height: calc(90vh - 70px) !important;
        }

        #bins-panel {
            width: 190px !important;
            font-size: 10px !important;
            bottom: 90px !important;
        }
    }

    @media screen and (max-width: 768px) {
        #stats-sidebar {
            width: 220px !important;
            max-height: calc(75vh - 70px) !important;
            top: 60px !important;
            right: 5px !important;
        }

        #bins-panel {
            width: 160px !important;
            bottom: 90px !important;
            left: 5px !important;
        }

        #stats-sidebar h2 {
            font-size: 14px !important;
        }

        #stats-sidebar h3 {
            font-size: 12px !important;
        }

        #bins-panel h4 {
            font-size: 11px !important;
        }
    }

    @media screen and (max-width: 480px) {
        #stats-sidebar {
            max-height: calc(70vh - 70px) !important;
            top: 60px !important;
            max-width: 280px !important;
            max-height: 60vh !important;
            top: 5px !important;
            right: 5px !important;
            padding: 8px !important;
        }

        #bins-panel {
            bottom: 90px !important;
            width: calc(100vw - 20px) !important;
            max-width: 200px !important;
            bottom: 25px !important;
            left: 5px !important;
            padding: 8px !important;
        }

        #stats-sidebar h2 {
            font-size: 13px !important;
        }

        #stats-sidebar h3 {
            font-size: 11px !important;
        }

        #bins-panel h4 {
            font-size: 10px !important;
        }
    }

    @media screen and (max-width: 360px) {
        #stats-sidebar {
            max-height: 50vh !important;
            font-size: 9px !important;
        }

        #bins-panel {
            font-size: 9px !important;
        }
    }

    #stats-sidebar, #bins-panel {
      transition: transform 260ms ease, opacity 260ms ease;
      will-change: transform;
    }

    #stats-sidebar { transform: translateX(0); }
    #bins-panel    { transform: translateX(0); }

    #stats-sidebar.collapsed {
      transform: translateX(110%);
      opacity: 0.98;
      pointer-events: none;
    }
    #bins-panel.collapsed {
      transform: translateX(-110%);
      opacity: 0.98;
      pointer-events: none;
    }

    #stats-sidebar.open, #bins-panel.open {
      pointer-events: auto;
    }

    #toggle-stats-btn, #toggle-bins-btn {
      position: absolute;
      z-index: 2000;
      border: 1px solid rgba(255,255,255,0.18);
      background: rgba(20, 20, 20, 0.92);
      color: #eaeaea;
      padding: 8px 10px;
      border-radius: 10px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      box-shadow: 0 6px 18px rgba(0,0,0,0.35);
      backdrop-filter: blur(6px);
    }

    #toggle-stats-btn {
      top: 10px;
      right: 10px;
    }

    #toggle-bins-btn {
      bottom: 40px;
      left: 10px;
    }

    @media screen and (min-width: 769px) {
      #toggle-stats-btn, #toggle-bins-btn {
        opacity: 0.75;
      }
      #toggle-stats-btn:hover, #toggle-bins-btn:hover {
        opacity: 1.0;
      }
    }
</style>
"""

    m.get_root().html.add_child(folium.Element(css))

    viewport_meta = (
        '<meta name="viewport" content="width=device-width, initial-scale=1.0, '
        'maximum-scale=1.0, user-scalable=no">'
    )
    m.get_root().header.add_child(folium.Element(viewport_meta))

    folium.LayerControl(position="topleft", collapsed=True).add_to(m)
    Fullscreen(position="topleft", title="Fullscreen", title_cancel="Exit Fullscreen").add_to(m)

    m.save(output_html_path)
