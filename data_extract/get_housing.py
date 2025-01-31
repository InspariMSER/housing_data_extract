from .housing import housing_main

def data_extract_main():
    """Main entry point for the housing data extraction application."""
    print("Starting housing data extraction...")
    housing_main()
    print("Housing data extraction completed.")

if __name__ == "__main__":
    data_extract_main()
