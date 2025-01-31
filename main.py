import subprocess
import sys

def install_requirements():
    """Install required packages from requirements.txt"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Requirements installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"Error installing requirements: {e}")
        sys.exit(1)

def main():
    """Main entry point for the housing data extraction application."""
    # Install requirements first
    install_requirements()
    
    # Import after installing requirements
    from data_extract import data_extract_main
    
    print("Starting housing data extraction...")
    data_extract_main()
    print("Housing data extraction completed.")

if __name__ == "__main__":
    main()
