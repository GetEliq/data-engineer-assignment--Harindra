from pathlib import Path
import sys
 
SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
 
from src.etl.etl_run import run_etl        
from src.sql.run_queries import run_queries   

def main():
    run_etl()
    run_queries()

if __name__ == "__main__":
    main()

