"""Command-line entry point for the separate NCD pipeline."""

import logging

from ncd.pipeline import main


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logging.error("NCD pipeline failed: %s", error)
        raise SystemExit(1)

