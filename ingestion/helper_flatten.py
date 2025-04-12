"""
Flattens nested JSON data into separate restaurant and menu files.
"""

import logging
from constants import INPUT_FILE, OUTPUT_FLAT_RESTAURANTS, OUTPUT_FLAT_MENU
from utils import flatten_json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main function to flatten JSON data."""
    try:
        restaurant_count, menu_item_count, duration, memory_mb = flatten_json(
            INPUT_FILE,
            OUTPUT_FLAT_RESTAURANTS,
            OUTPUT_FLAT_MENU
        )

        logger.info(f"📄 Output Files: {OUTPUT_FLAT_RESTAURANTS}, {OUTPUT_FLAT_MENU}")

    except Exception as e:
        logger.error(f"Error flattening JSON: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()