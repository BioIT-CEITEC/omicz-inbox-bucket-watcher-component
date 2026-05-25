"""Entry point for running s3_watcher as a module.

Usage:
    python -m src --config config.yaml
    python -m src --once --log-level DEBUG
"""

from .cli import main

if __name__ == "__main__":
    main()
