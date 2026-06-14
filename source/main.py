"""
DataMind AI — Source Systems Simulation

Entry point for running all 7 enterprise source system Kafka producers.
Default mode streams all systems with realistic event distribution.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from runners.run_all import main as run_all_main


def main():
    run_all_main()


if __name__ == "__main__":
    main()
