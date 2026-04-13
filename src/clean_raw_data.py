import os
import re

import asyncio
import aiohttp
import uuid
import hashlib

from datetime import datetime
import pandas as pd
from typing import Optional
from prefect import flow, task

from gc_utility.read_data import read_csv_from_gcs
from gc_utility.upload_data import upload_csv_to_gcs

# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================

BUCKET_NAME = os.getenv("NUMISMATIC_BUCKET")
SOURCE_BLOB_NAME = os.getenv("NUMISMATIC_RAW")
NUMISMATIC_CLEAN = os.getenv("NUMISMATIC_CLEAN")

# ============================================================================
# CONSTANTS AND REFERENCE DATA
# ============================================================================


HISTORICAL_ENTITIES = {
    # Europe and Empares
    "URSS": "Non-existent",
    "Yugoslavia": "Non-existent",
    "Checoslovaquia": "Non-existent",
    "Prusia": "Non-existent",
    "Imperio Austro-Húngaro": "Non-existent",
    "Imperio Otomano": "Non-existent",
    "República Democrática Alemana": "Non-existent",
    "Ciudad Libre de Danzig": "Non-existent",
    "Sarre": "Non-existent",
    "Imperio Ruso": "Non-existent",
    "Bohemia y Moravia": "Non-existent", 
    "Estado Libre Irlandés": "Non-existent",

    # America
    "Gran Colombia": "Non-existent",
    "Provincias Unidas del Centro de América": "Non-existent",
    "Confederación Argentina": "Non-existent",
    "Estados Confederados de América": "Non-existent",
    "República de Texas": "Non-existent",
    "Antillas Neerlandesas": "Historical Colony",
    "Guyana Británica": "Historical Colony",
    "Honduras Británica": "Historical Colony",
    "Guayana Holandesa": "Historical Colony", 
    "Indias Occidentales Danesas": "Historical Colony",
    "Terranova": "Historical Colony", 

    # Asia and Oceanía
    "Indochina Francesa": "Historical Colony",
    "Indias Orientales Neerlandesas": "Historical Colony",
    "Vietnam del Sur": "Non-existent",
    "Siam": "Non-existent",
    "Ceilán": "Non-existent",
    "Nueva Guinea Alemana": "Historical Colony",
    "Estrechos de Malaca": "Historical Colony",
    "Malaya y Borneo Británico": "Historical Colony",
    "Malaya": "Historical Colony",
    "Birmania": "Non-existent", 
    "Sarawak": "Historical Colony",

    # Africa
    "Zaire": "Non-existent",
    "Rodesia": "Non-existent",
    "África Occidental Francesa": "Historical Colony",
    "África Ecuatorial Francesa": "Historical Colony",
    "África Oriental Alemana": "Historical Colony",
    "Congo Belga": "Historical Colony",
    "Biafra": "Non-existent",
    "Katanga": "Non-existent",
    "Tanganica": "Non-existent",
    "Zanzíbar": "Non-existent",
    "Unión Sudafricana": "Non-existent",
    "Alto Volta": "Non-existent",
    "Ruanda-Urundi": "Historical Colony",
    "Dahomey": "Non-existent", 
    "África Oriental Italiana": "Historical Colony",
    "África del Sudoeste": "Historical Colony" 
}

WAR_KEYWORDS = [
    "ocupacion nazi",
    "ocupacion japonesa",
    "ocupacion francesa",
    "ocupacion inglesa",
    "ocupacion aliada",
    "ocupacion rusa",
    "ocupacion alemana",
    "german occupation",
    "nazi occupation",
    "guerra civil",
    "fuerzas armadas",
    "armada sovietica",
    "territorios ocupados",
    "wwii",
    "wwi",
    "ejercito",
    "war",
    "militar",
    "nazi",
    "ocupacion"
]
    
UNIT_SINGULAR_MAP = {
    # Spanish
    'pesos': 'Peso',
    'centavos': 'Centavo',
    'dolares': 'Dolar',
    'dólares': 'Dolar',
    'reales': 'Real',
    'bolivianos': 'Boliviano',
    'guaranies': 'Guarani',
    'soles': 'Sol',
    'quetzales': 'Quetzal',
    'lempiras': 'Lempira',
    'colones': 'Colon',
    'cordobas': 'Cordoba',
    'balboas': 'Balboa',
    'sucres': 'Sucre',
    'pesetas': 'Peseta',
    'liras': 'Lira',
    'marcos': 'Marco',
    'francos': 'Franco',
    'libras': 'Libra',
    'yuanes': 'Yuan',
    'rublos': 'Rublo',
    'dinares': 'Dinar',
    'riales': 'Rial',
    'dirhams': 'Dirham',
    'shillings': 'Shilling',
    'pounds': 'Pound',
    'dollars': 'Dollar',
    'cents': 'Cent',
    'euros': 'Euro',
    'centimos': 'Centimo',
    'australes': 'Austral',
    'intis': 'Inti',
    'centesimos': 'Centesimo', 
    'peniques': 'Penique',
    
    # English
    'marks': 'Mark',
    'francs': 'Franc',
    'guilders': 'Guilder',
    'florins': 'Florin',
    'thalers': 'Thaler',
    'crowns': 'Crown',
    'pennies': 'Penny',
    'kopeks': 'Kopek',
    'kopecks': 'Kopek',
    'dinars': 'Dinar', 
    'rupees': 'Rupee',
    'annas': 'Anna',   
    
    # Others 
    'cruzeiros': 'Cruzeiro',
    'cruzados': 'Cruzado',
    'kwanzas': 'Kwanza',
    'nairas': 'Naira',
    'cedis': 'Cedi',
    'cfa': 'CFA',
    'afganis': 'Afghani',
    'tugriks': 'Tugrik',
    'kips': 'Kip',
    'riels': 'Riel',
    'somoni': 'Somoni',
    'manats': 'Manat',
    'tenge': 'Tenge',
    'vatu': 'Vatu',
    'paanga': 'Paanga',
    'billetes': 'Billete', 
    'test': 'Test',  
    'trillones': 'Trillon',
    'nuevos': 'Nuevo',  
    'shekels': 'Shekel',
    'rupias': 'Rupia',  
    'korun': 'Koruna',  
    'kronor': 'Krona',  
    'kronen': 'Krone',  
    'kronur': 'Krona',  
    'leke': 'Leke',
    'escudos': 'Escudo',
    'reis': 'Real',
    'patacas': 'Pataca',
    'dong': 'Dong',
    'rand': 'Rand',
    'kwacha': 'Kwacha',
    'francos': 'Franco',
    'piastres': 'Piastre',
    'piastre': 'Piastre',
    'taka': 'Taka',
    'riyals': 'Riyal',
    'ringgit': 'Ringgit',
    'pula': 'Pula',
    'leu': 'Leu',
    'lei': 'Leu',
    'zlotych': 'Zloty',     
    'kune': 'Kuna',         
    'schillinge': 'Schilling', 
    'leva': 'Lev',          
    'centesimi': 'Centesimo', 
    'centimes': 'Centime',  
    'paras': 'Para',        
}

# Complete list of possible tags (ordered by priority)
EXTRA_TAGS = [
    "Agujeros", "Aniversario", "Numero Bajo", "Tesoro Nacional", 
    "FIFA", "Mundial Futbol", "Error", "Copia", "Polimero", 
    "Pegado", "Proof", "Fantasia", "Certificado", "Reparado-Cinta", 
    "MADERA", "Manchado", "Escrito", "Roto", "Rota", "Roturas", 
    "Rasgado", "Cancelado", "Reposición", "Sin Circular", "BC", 
    "SC", "FDC", "EBC", "MBC"
]

# Country aliases for more flexible matching
COUNTRY_ALIASES = {
    "uk": "Gran Bretaña",
    "usa": "Estados Unidos",
    "eeuu": "Estados Unidos"
}

# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

def check_war_context(title: str) -> bool:
    """Check if the title contains war-related keywords.
    
    Args:
        title (str): The product title to analyze
        
    Returns:
        bool: True if war-related keywords are found, False otherwise
    """
    
    if not isinstance(title, str):
        return False
    
    title_lower = title.lower()
    return any(keyword in title_lower for keyword in WAR_KEYWORDS)


def extract_country_data(title: str) -> dict:
    """
      Extract country name, historical status, and war context from title.
    
    Args:
        title (str): The product title
        
    Returns:
        dict: Dictionary containing:
            - Country (str): Extracted country name
            - Status (str): "Existente" or "Entidad Desaparecida"
            - War (bool): True if war-related
    """
    
    if not isinstance(title, str) or not title.strip():
        return {"country": "Unknown", "status": "Existent", "is_war": False}
    
    normalized = title.lower().strip()

    # War Normalization and Detection
    is_war = check_war_context(title)

    # Cleaning of sales PREFIXES (Lot, Set, Replica)
    # Removing words that are not the country but are at the beginning
    clean_text = re.sub(r'^(lote de|set de|coleccion de|billete replica de|billete de|replica de)\s+', '', normalized)

    # Cleaning of institutional PREFIXES (Banco, gobierno, etc.)
    clean_text = re.sub(r'^(banco de|banco|gobierno de|government of)\s+', '', clean_text)

    # MASTER EXTRACTION (Everything before the keyword "Ticket" or similar)
    # Example: "Indochina Francesa Billete..." -> "Indochina Francesa"
    match = re.search(r'^(.*?)\s+(billete|billlete|set|lote|bono|check|cheque)', clean_text)
    
    candidate = ""
    if match:
        candidate = match.group(1).strip()
    else:
        # If you don't find the word "Ticket", take the first two words in case it's a compound word.
        #But if the second one is a number (year), it only takes the first one.
        words = clean_text.split()
        if len(words) >= 2 and not words[1].isdigit():
            candidate = f"{words[0]} {words[1]}"
        elif len(words) > 0:
            candidate = words[0]

    #Final normalization with Aliases and Capitalization
    country_name = COUNTRY_ALIASES.get(candidate, candidate).title()
    
    #Determine Historical Status
    status = HISTORICAL_ENTITIES.get(country_name, "Existent")

    return {
        "Country": country_name,
        "Status": status,
        "War": is_war
    }

def extract_banknote_details(title: str) -> dict:
    """
        Extract denomination value and currency unit from title.
    
    Args:
        title (str): The product title
        
    Returns:
        dict: Dictionary containing:
            - DenomValue (float): Numerical denomination
            - DenomUnit (str): Currency unit in singular form
    """

    title_no_year = re.sub(r'\b(18|19)\d{2}\b', '', title)
    
    match = re.search(
        r'(\d{1,3}(?:[.,]\d{3})*(?:\.\d{2})?|\d{1,4})\s+([a-zA-ZáéíóúÁÉÍÓÚñÑ]+)',
        title_no_year
    )
    
    if match:
        val_str = match.group(1).replace('.', '').replace(',', '.')
        unit_raw = match.group(2).capitalize()
        
        unit_normalized = UNIT_SINGULAR_MAP.get(unit_raw.lower(), unit_raw)
        
        return {
            "DenomValue": float(val_str),
            "DenomUnit": unit_normalized  
        }
    
    return {"DenomValue": None, "DenomUnit": "Unknown"}

def extract_year(title: str) -> Optional[str]:
    """
    Extract year from title (4-digit number between 1800-2100).
    
    Args:
        title (str): The product title
        
    Returns:
        int or None: Extracted year or None if not found
    """
    
    try:
        matches = re.findall(r"\b([1-2]\d{3})\b", title)
        if matches:
            # Return the first valid year (typically the most relevant)
            for year_str in matches:
                year = int(year_str)
                if 1800 <= year <= 2100:
                    return year_str
        return None
    except Exception as e:
        return None


def extract_condition(title: str) -> str:
    """    
    Extract condition grade from title.
    
    Args:
        title (str): The product title
        
    Returns:
        str: Condition grade in uppercase (UNC, AU, XF, etc.) or "UNKNOWN"
    """
    
    match = re.search(r"\b(UNC|AU|XF|VF|F|VG|G|PO|FR|AG)\b", title, re.IGNORECASE)
    return match.group(1).upper() if match else "UNKOWN"


def extract_series(title: str) -> Optional[str]:
    """
    Extract series information from title.
    
    Args:
        title (str): The product title
        
    Returns:
        str or None: Series identifier (e.g., "Serie A") or None
    """
    match = re.search(r"\(Serie\s+([A-Za-z0-9]+)\)", title, re.IGNORECASE)
    if match:
        return f"Serie {match.group(1).upper()}"
    return None

def extract_extra_info(title: str) -> Optional[list]:
    """
    Extract all extra characteristics from title.
    
    Args:
        title (str): The product title
        
    Returns:
        list or None: List of tags (e.g., ["Polimero", "Aniversario"]) or None
    """
    if not isinstance(title, str) or not title.strip():
        return None
    
    
    # Join tags with | for regex (escape special characters)
    tags_pattern = '|'.join(re.escape(tag) for tag in EXTRA_TAGS)
    
    # Alternative patterns for different formats
    patterns = [
        rf'\((?:{tags_pattern})(?:\s*[A-Za-z0-9\-]*)?\)',  
        rf'\[(?:{tags_pattern})(?:\s*[A-Za-z0-9\-]*)?\]',  
        rf'\b(?:{tags_pattern})\b',                         
    ]
    
    found_tags = []
    
    for pattern in patterns:
        matches = re.findall(pattern, title, re.IGNORECASE)
        for match in matches:
            # Clean and normalize each tag found
            tag_clean = re.sub(r'[\[\]()]', '', match).strip()
            
            # Find the base tag in our list
            for valid_tag in EXTRA_TAGS:
                if valid_tag.lower() in tag_clean.lower():
                    # Normalize to standard format
                    tag_normalized = valid_tag
                    # Extract additional value if it exists
                    extra_val = re.sub(rf'{valid_tag}\s*', '', tag_clean, flags=re.IGNORECASE).strip()
                    if extra_val:
                        tag_normalized = f"{valid_tag} {extra_val.upper()}"
                    
                    # Avoid duplicates
                    if tag_normalized not in found_tags:
                        found_tags.append(tag_normalized)
                    break
    
    return found_tags if found_tags else None


def extract_century(year: int) -> Optional[str]:
    
    """
    Calculate century based on year.
    
    Args:
        year (int): Year
        
    Returns:
        str or None: Century in Roman numerals or None
    """
    
    if year is None:
        return None
    
    if 1800 <= year < 1900:
        return 'XIX'
    elif 1900 <= year < 2000:
        return 'XX'
    elif 2000 <= year < 2100:
        return 'XXI'
    else:
        return 'Previous to XIX'
    
def clean_price(price: str) -> Optional[float]:
    """
    Clean and convert price string to float.
    
    Args:
        price (str): Price string (e.g., "$100.50", "100,50 ARS")
        
    Returns:
        float or None: Cleaned price or None if parsing fails
    """
    try:
        if not isinstance(price, str) or price.strip() == "No price found":
            return None
            
        # Remove common currency symbols and letters
        cleaned = re.sub(r"[^\d.,]", "", price.strip())
        
        # Handle different decimal separators (European: comma, US: dot)
        if "," in cleaned and "." in cleaned:
            # Assume last separator is decimal
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            # European format: 1.000,50 or just 100,50
            if cleaned.count(",") == 1 and cleaned.count(".") == 0:
                cleaned = cleaned.replace(",", ".")
            else:
                cleaned = cleaned.replace(".", "").replace(",", ".")
        
        return float(cleaned) if cleaned else None
        
    except Exception as e:
        return None
    
def generate_primary_key(title: str, link: str, country: str = None, year: int = None) -> str:
    """
    Generate a unique deterministic product key using MD5 hash.
    This ID identifies the PRODUCT, not the price snapshot.
    The same product will always generate the same ID across runs.
    
    Args:
        title (str): Product title
        link (str): Product URL
        country (str, optional): Country name
        year (int, optional): Year
        
    Returns:
        str: Product key in format "COUNTRY_YEAR_HASH"
        
    Examples:
        >>> generate_primary_key("Cuba Billete...", "https://...", "Cuba", 1959)
        'CUB_1959_a3f5c8d9e2b1f4a6c7d8e9f0a1b2c3d4'
    """
    
    country_code = (country[:3] if country else "UNK").upper()
    year_str = str(year) if year else "0000"
    
    unique_string = f"{title.strip()}|{link.strip()}"
    unique_hash = hashlib.md5(unique_string.encode()).hexdigest()
    
    return f"{country_code}_{year_str}_{unique_hash}"
 
 
def generate_snapshot_id(product_id: str, scraped_at: str) -> str:
    """
    Generate a unique ID for each price snapshot (product + date).
    This allows tracking price changes over time for the same product.
    
    Args:
        product_id (str): The product's stable ID (from generate_primary_key)
        scraped_at (str): ISO timestamp of when the snapshot was taken
        
    Returns:
        str: Snapshot ID in format "PRODUCT_ID_YYYYMMDD"
        
    Examples:
        >>> generate_snapshot_id("CUB_1959_abc123", "2024-03-15T10:30:00")
        'CUB_1959_abc123_20240315'
    """
    date_str = scraped_at[:10].replace("-", "")  # "YYYY-MM-DD" -> "YYYYMMDD"
    return f"{product_id}_{date_str}"
 
 
def add_snapshot_columns(df: pd.DataFrame, scraped_at: str) -> pd.DataFrame:
    """
    Add snapshot tracking columns to a transformed DataFrame.
    
    - scraped_at : ISO timestamp of this scraping run
    - snapshot_id: unique ID per product per day (product_id + date)
                   Prevents duplicate inserts if the flow runs twice in a day.
    
    Args:
        df (pd.DataFrame): Transformed DataFrame that already has an 'id' column
        scraped_at (str): ISO timestamp string, e.g. "2024-03-15T10:30:00"
        
    Returns:
        pd.DataFrame: Same DataFrame with two new columns prepended
    """
    df = df.copy()
    df["scraped_at"] = scraped_at
    df["snapshot_id"] = df["id"].apply(
        lambda product_id: generate_snapshot_id(product_id, scraped_at)
    )
    return df


# ============================================================================
# PREFECT FLOW
# ============================================================================

@task
def load_raw_data(bucket: str, source: str) -> pd.DataFrame:
    
    """    
    Load raw data from Google Cloud Storage.
    
    Args:
        bucket (str): GCS bucket name
        source (str): Blob path
        
    Returns:
        pd.DataFrame: Raw data DataFrame
    """
    df = read_csv_from_gcs(bucket, source)
    return df

@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
  
    """
    Apply all transformations to extract structured data.
    
    Args:
        df (pd.DataFrame): Raw data with 'title', 'price', 'link' columns
        
    Returns:
        pd.DataFrame: Transformed data with all extracted fields
        
    Transformations applied:
        1. Extract country, status, war context
        2. Extract denomination value and unit
        3. Extract year and century
        4. Extract condition grade
        5. Extract series information
        6. Extract extra tags
        7. Clean price
        8. Generate primary key
        9. Initialize availability column
    """
    
    df = df.copy()
    
    # Extract Country, Status e Is_War
    country_info = df["title"].apply(extract_country_data).apply(pd.Series)
    df = pd.concat([df, country_info], axis=1)
    
    # Extract Value y Unit
    denom_info = df["title"].apply(extract_banknote_details).apply(pd.Series)
    df = pd.concat([df, denom_info], axis=1)
    
    df["Year"] = df["title"].apply(extract_year)
    df["Condition"] = df["title"].apply(extract_condition)  
    
    df["ExtraTags"] = df["title"].apply(extract_extra_info).apply(
        lambda x: '; '.join(x) if x and isinstance(x, list) else None
    )
    df["Price"] = df["price"].apply(clean_price)
    
    df["id"] = df.apply(
        lambda row: generate_primary_key(
            row["title"], 
            row["link"], 
            row.get("Country"), 
            row.get("Year")
        ), 
        axis=1
    )
    
    df["Century"] = df["Year"].apply(lambda y: extract_century(int(y)) if y else None)
    
    if "price" in df.columns:
        df = df.drop(columns=["price"])
    
    if "title" in df.columns:
        df = df.drop(columns=["title"])
    
    if "link" in df.columns:
        df = df.drop(columns=["link"])
    
    df = df.dropna(subset=["Price"])
    
    column_order = [
        'idSnapshot', 'id', 'ScrapedAt',
        'Country', 'Status', 'War', 'DenomValue', 'DenomUnit',
        'Year', 'Century', 'Condition', 'Series', 'ExtraTags',
        'Price'
    ]
    
    existing_cols = [col for col in column_order if col in df.columns]
    df = df[existing_cols]
    
    return df.dropna(subset=["Price"])


@flow
async def clean_data_flow():
    """
    Main ETL flow for numismatic data processing (price history mode).
    
    Each run appends a new snapshot of all current prices instead of
    overwriting. This allows tracking how prices change over time.
    
    Flow:
        1. Record the current timestamp (all rows in this run share it)
        2. Load raw data from GCS
        3. Transform and extract structured fields
        4. Add snapshot columns (scraped_at, snapshot_id)
        5. Filter out any snapshots already stored (idempotency guard)
        6. Append new snapshots to the existing clean dataset in GCS
        
    Returns:
        pd.DataFrame: Only the newly added snapshots
    """
    scraped_at = datetime.utcnow().isoformat()
    print(f"Starting scraping run: {scraped_at}")
    
    # 1. Load raw data
    df_raw = await load_raw_data(BUCKET_NAME, SOURCE_BLOB_NAME)
    
    # 2. Transform and add snapshot IDs
    df_transformed = transform_data(df_raw)
    df_final = add_snapshot_columns(df_transformed, scraped_at)
    
    if len(df_final) == 0:
        print("No data extracted in this run.")
        return df_final
    
    # 3. Partitioning logic: Create a unique filename based on current date
    date_str = datetime.utcnow().strftime('%Y%m%d')
    file_name = f"clean_banknotes_{date_str}.csv"
    gcs_destination = f"clean/{file_name}"
    
    # 4. Save locally and upload to GCS
    df_final.to_csv(file_name, index=False)
    await upload_csv_to_gcs(BUCKET_NAME, file_name, gcs_destination)
    #await upload_csv_to_gcs(BUCKET_NAME, "billetes_clean.csv", NUMISMATIC_CLEAN)
    
    return df_final
    
    
# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    asyncio.run(clean_data_flow())
    
 
