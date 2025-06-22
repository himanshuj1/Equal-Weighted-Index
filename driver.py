import os
import sys
from pathlib import Path
from datetime import datetime

# Set up path to access scripts in the /scripts directory
CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR / "scripts"
sys.path.append(str(SCRIPTS_DIR))

# Import scripts as modules
import fetch_top100_marketcap as step1
import construct_index as step2
import track_composition as step3
import analysis as step4

def run_pipeline():
    print("\nStarting the Custom Index Tracker Pipeline...")
    start_time = datetime.now()

    print("\n[1/4] Fetching top 100 market cap stocks...")
    step1.main()

    print("\n[2/4] Constructing the index...")
    step2.main()

    print("\n[3/4] Tracking index composition...")
    step3.main()

    print("\n[4/4] Running analysis and exporting to Excel...")
    step4.export_all_to_excel()

    end_time = datetime.now()
    print(f"\nPipeline completed in {end_time - start_time}")

if __name__ == "__main__":
    run_pipeline()
