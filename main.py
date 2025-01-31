%pip install -r requirements.txt

def main():
    """Main entry point for the housing data extraction application."""
    
    # Import after installing requirements
    from data_extract import data_extract_main
    
    print("Starting housing data extraction...")
    data_extract_main()
    print("Housing data extraction completed.")

if __name__ == "__main__":
    main()
