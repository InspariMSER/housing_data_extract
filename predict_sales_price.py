import datetime
from main_dec import main
from utils import predict_sales_price
from delta_utils import read_from_delta

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

def get_available_zip_codes() -> list[str]:
    """Get list of available zip codes from Delta table."""
    df = read_from_delta("salesprices")
    return sorted(df['zip_code'].unique().tolist())

def select_zip_code() -> str:
    """Let user select a zip code."""
    zip_codes = get_available_zip_codes()
    if not zip_codes:
        raise ValueError("No sales price data found in Delta table")
    
    print("\nAvailable zip codes:")
    for i, zip_code in enumerate(zip_codes):
        print(f"{i+1}: {zip_code}")
    
    while True:
        try:
            choice = int(input("\nSelect zip code number: ")) - 1
            if 0 <= choice < len(zip_codes):
                return zip_codes[choice]
            print("Invalid selection. Please choose from the list above.")
        except ValueError:
            print("Please enter a valid number.")

@main
def _() -> None:
    """
    Predict house price based on scraped sales data.
    """
    zip_code = select_zip_code()
    house_area = get_house_area()
    
    prices = read_from_delta("salesprices")
    prices = prices[prices['zip_code'] == zip_code]
    
    price = predict_sales_price(
        prices=prices,
        house_area=house_area,
        min_sales_year=datetime.datetime.today().year - 2,
        max_sales_year=datetime.datetime.today().year,
        min_area=house_area * 0.7,
        max_area=house_area * 1.3,
        min_samples=3
    )
    
    formatted_price = '{:,.0f} kr'.format(price)
    print(f"\nSuggested price for {house_area}m² house in {zip_code}: {formatted_price}")
