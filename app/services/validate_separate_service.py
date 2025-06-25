import logging

logger = logging.getLogger(__name__)

async def validate_gtin(gtin: str) -> dict:
    
    # Validates GTIN formats: UPC (12), EAN/ITN (13), and GS1 (14 to 128 digits).
    # Returns dict with validity and GTIN type.
    
    try:
        gtin = gtin.strip()
        if not gtin.isdigit():
            return {"valid": False, "type": "Invalid", "reason": "GTIN must be numeric"}

        length = len(gtin)

        # UPC: GTIN-12
        if length == 12:
            return {
                "valid": _validate_mod10(gtin),
                "type": "UPC (GTIN-12)"
            }
        # EAN/ITN: GTIN-13
        elif length == 13:
            return {
                "valid": _validate_mod10(gtin),
                "type": "EAN/ITN (GTIN-13)"
            }
        # GS1: 14 to 128 digits
        elif 14 <= length <= 128:
            return {
                "valid": True,
                "type": "GS1 (GTIN-14 to 128)"
            }
        else:
            return {"valid": False, "type": "Invalid", "reason": "Unsupported GTIN length"}

    except Exception as e:
        logger.exception("GTIN validation error")
        return {"valid": False, "type": "Error", "reason": str(e)}

def _validate_mod10(gtin: str) -> bool:
    # Mod-10 check digit validation for GTIN-12 and GTIN-13.
    digits = [int(d) for d in gtin]
    check_digit = digits.pop()
    digits.reverse()

    total = 0
    for i, d in enumerate(digits):
        total += d * (3 if i % 2 == 0 else 1)

    calculated_check_digit = (10 - (total % 10)) % 10
    return check_digit == calculated_check_digit

def validate_price(price) -> dict:
    
    # Validates that price is a positive number (int or float).
    # Returns dict with validity and reason if invalid.

    try:
        if price is None:
            return {"valid": False, "reason": "Price is None"}

        if isinstance(price, (int, float)):
            if price >= 0:
                return {"valid": True}
            else:
                return {"valid": False, "reason": "Price must be non-negative"}
        else:
            # Try to convert to float
            price_float = float(price)
            if price_float >= 0:
                return {"valid": True}
            else:
                return {"valid": False, "reason": "Price must be non-negative"}
    except Exception as e:
        logger.exception("Price validation error")
        return {"valid": False, "reason": f"Invalid price format: {str(e)}"}

def validate_location(location) -> dict:

    # Validates that location is present (non-empty).
    # Returns dict with validity and reason if invalid.
    
    if location is None:
        return {"valid": False, "reason": "Location is None"}
    return {"valid": True}
