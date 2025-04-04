from data_extract.skoler.overblik import get_overblik

def get_school_data():
    """Get school overview data."""
    return get_overblik()

__all__ = ['get_school_data']
