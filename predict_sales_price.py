import datetime
from pathlib import Path
from main_dec import main
from utils import read_prices, predict_sales_price

def get_house_area() -> int:
    """Get house area from user input."""
    while True:
        try:
            area = int(input("Enter house area in m2: "))
            if area > 0:
                return area
            print("Area must be greater than 0.")
        except ValueError:
            print("Please enter a valid number.")

def get_available_data_files() -> list[Path]:
    """Get list of available sales price data files."""
    data_dir = Path('data')
    return list(data_dir.glob('sales_prices_*.csv'))

def select_data_file() -> Path:
    """Let user select a data file."""
    files = get_available_data_files()
    if not files:
        raise FileNotFoundError("No sales price data files found in data directory")
    
    print("\nAvailable zip codes:")
    for i, file in enumerate(files):
        zip_code = file.stem.split('_')[-1]
        print(f"{i+1}: {zip_code}")
    
    while True:
        try:
            choice = int(input("\nSelect zip code number: ")) - 1
            if 0 <= choice < len(files):
                return files[choice]
            print("Invalid selection. Please choose from the list above.")
        except ValueError:
            print("Please enter a valid number.")

@main
def _() -> None:
    """
    Predict house price based on scraped sales data.
    """
    data_file = select_data_file()
    house_area = get_house_area()
    
    prices = read_prices(str(data_file))
    price = predict_sales_price(
        prices=prices,
        house_area=house_area,
        min_sales_year=datetime.datetime.today().year - 2,  # Last 2 years
        max_sales_year=datetime.datetime.today().year,
        min_area=house_area * 0.7,  # ±30% of target area
        max_area=house_area * 1.3,
        min_samples=3
    )
    
    formatted_price = '{:,.0f} kr'.format(price)
    zip_code = data_file.stem.split('_')[-1]
    print(f"\nSuggested price for {house_area}m² house in {zip_code}: {formatted_price}")
