import os
from datetime import datetime

def check_daily_run():
    flag_file = 'daily_run.txt'
    today = datetime.now().strftime('%y%m%d')
    
    # Check if file exists
    if os.path.exists(flag_file):
        with open(flag_file, 'r') as f:
            last_run = f.read().strip()
        
        # If last run was today, return False
        if last_run == today:
            return False
    
    # If file doesn't exist or last run wasn't today
    with open(flag_file, 'w') as f:
        f.write(today)
    return True