from data_extract.skoler import get_school_data
import logging

def main():
    """Main function to get school data."""
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting school data extraction")
    
    try:
        data = get_school_data()
        logging.info(f"Successfully extracted data for {len(data)} schools")
    except Exception as e:
        logging.error(f"Error extracting school data: {e}")
        raise

if __name__ == "__main__":
    main()
