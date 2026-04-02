"""
Configuration Module for Electricity Cost Optimization

Contains all configuration constants, API settings, and default parameters.
"""

import os
from pathlib import Path

# ==================== DIRECTORY PATHS ====================
PROJECT_ROOT = Path(__file__).parent.parent
BASE_DIR = PROJECT_ROOT
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
ARCHIVE_DIR = DATA_DIR / "archive"
TEMP_DIR = DATA_DIR / "temp"
HOURLY_RESULTS_DIR = DATA_DIR / "hourly_results"
PRICES_DIR = DATA_DIR / "prices"
DA_PRICES_DIR = PRICES_DIR / "DA_prices"
FUTURES_DIR = PRICES_DIR / "futures"
PFC_DIR = PRICES_DIR / "PFC"
PPA_DIR = DATA_DIR / "ppa"
CONFIG_DIR = BASE_DIR / "config"

# Dictionary interface for easy access
DIRS = {
    'project_root': PROJECT_ROOT,
    'data': DATA_DIR,
    'input': INPUT_DIR,
    'output': OUTPUT_DIR,
    'archive': ARCHIVE_DIR,
    'temp': TEMP_DIR,
    'hourly_results': HOURLY_RESULTS_DIR,
    'prices': PRICES_DIR,
    'da_prices': DA_PRICES_DIR,
    'futures': FUTURES_DIR,
    'pfc': PFC_DIR,
    'ppa': PPA_DIR,
    'config': CONFIG_DIR
}

# ==================== API CONFIGURATION ====================
# ENTSO-E API
ENTSOE_API_TOKEN = 'b2432805-be69-481b-a32d-b850a6f11aa6'
ENTSOE_BIDDING_ZONE = 'DE_LU'

# Montel API
MONTEL_API_BASE_URL = "https://api.montelnews.com"
MONTEL_FUNDAMENTAL_ENDPOINT = "/fundamental/get"
MONTEL_DERIVATIVES_ENDPOINT = "/derivatives/quote/get"

# Database Configuration
DB_URL = 'postgresql://retool:hOc2JYpWU6wn@ep-summer-mode-114239.us-west-2.retooldb.com/retool?sslmode=require'
TOKEN_QUERY = 'SELECT token FROM token'

# ==================== OPTIMIZATION PARAMETERS ====================
# Futures Products
FUTURES_PRODUCTS = ['1', '2', '3', '4', 'Y']  # Quarterly products + Yearly
FUTURES_TYPES = ['base', 'peak']

# PFC Configuration
PFC_FUNDAMENTAL_KEY = "PRICEIT_POWER_HPFC_EEX_DE_GREEN_SETTLEMENT_DYN"

# Futures Products for API
FUTURES_API_PRODUCTS = ['EEX DEB', 'EEX DEP']  # Base and Peak products

# ==================== DEFAULT PARAMETERS ====================
DEFAULT_MIN_TRANCHE_SIZE = 1.0
DEFAULT_HEDGE_FRACTION = 0.8
DEFAULT_PPA_FRACTION = 0.2
DEFAULT_SPOT_SOLD_LIMIT = 0.1

# ==================== TIME SETTINGS ====================
TIMEZONE = 'Europe/Berlin'
UTC_TIMEZONE = 'UTC'

# ==================== FILE SETTINGS ====================
TOKEN_FILE = 'token.csv'
RENEWABLES_FILE = 'renewables_data.csv'
DAILY_RUN_FILE = 'daily_run.txt'

# ==================== RETRY SETTINGS ====================
MAX_API_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# ==================== UTILITY FUNCTIONS ====================
def initialize_directories():
    """Create all necessary directories if they don't exist."""
    directories = [
        DATA_DIR, INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR, TEMP_DIR,
        HOURLY_RESULTS_DIR, PRICES_DIR, DA_PRICES_DIR, FUTURES_DIR,
        PFC_DIR, PPA_DIR, CONFIG_DIR
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        
def get_daily_date():
    """Get today's date in YYMMDD format."""
    from datetime import datetime
    return datetime.now().strftime('%y%m%d')

def get_current_year():
    """Get current year."""
    from datetime import datetime
    return datetime.now().year

# ==================== PATH HELPERS ====================
def get_data_path(relative_path):
    """Get absolute path for data files."""
    return DATA_DIR / relative_path

def get_input_path(filename):
    """Get absolute path for input files."""
    return INPUT_DIR / filename

def get_output_path(filename):
    """Get absolute path for output files."""
    return OUTPUT_DIR / filename

def get_prices_path(subdir, filename):
    """Get absolute path for price files."""
    return PRICES_DIR / subdir / filename 