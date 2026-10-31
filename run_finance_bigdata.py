import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def executer_script(nom_script):
    chemin = BASE_DIR / nom_script

    print(f"\nEXECUTION : {nom_script}", flush=True)

    subprocess.run(
        [sys.executable, str(chemin)],
        cwd=str(BASE_DIR),
        check=True
    )

def main():
    print("DEMARRAGE FINANCE_BIGDATA", flush=True)

    executer_script("src/etl/pipeline.py")
    executer_script("src/02_lire_transactions.py")
    executer_script("export_powerbi.py")

    print("\nPIPELINE FINANCE_BIGDATA TERMINE AVEC SUCCES")

if __name__ == "__main__":
    main()