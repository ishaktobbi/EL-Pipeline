from datetime import datetime
import re



VALID_TAXI_COLORS = {"yellow", "green"}



def parse_date(execution_date: str) -> tuple[str, str]:
        
        if not isinstance(execution_date, str):
            raise TypeError(f"❌ execution_date must be a str, got {type(execution_date).__name__}")
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", execution_date):
            raise ValueError(f"❌ Invalid date format: '{execution_date}'. Expected 'YYYY-MM-DD'.")
        
        try:
            date_strip = execution_date.strip()
            date_obj = datetime.strptime(date_strip, "%Y-%m-%d")
            return date_obj.strftime("%Y"), date_obj.strftime("%m")
        except ValueError as e:
            raise ValueError(f"❌ Invalid calendar date '{execution_date}': {e}") from e

def validate_color(taxi_color: str) -> str:
        
        if not isinstance(taxi_color, str):
            raise TypeError(f"❌ taxi color must be a str, got {type(taxi_color).__name__}")
        
       
        color = taxi_color.lower().strip()
        if color not in VALID_TAXI_COLORS:
            raise ValueError(f"❌ Invalid taxi color '{taxi_color}'. Allowed: {VALID_TAXI_COLORS}")
        
        return color
        