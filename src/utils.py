# src/utils.py
from datetime import datetime

def compute_remaining_lease(row):
    lease_start_year = int(row['lease_commence_date'])
    current_year = datetime.now().year
    
    remaining_years = 99 - (current_year - lease_start_year)
    
    years = max(remaining_years, 0)
    months = 0
    
    return f"{years} years {months} months"