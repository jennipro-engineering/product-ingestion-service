import json
from pydantic import ValidationError
from fastapi import APIRouter, Request, status, HTTPException
from app.config.database import get_database
import motor.motor_asyncio
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.models.ingestion_model import IngestionPayload
from app.services.pub_sub_service import send_to_pubsub
from app.config.settings import get_settings
from app.services.auth_service import verify_token
from app.services.validate_separate_service import validate_gtin, validate_price, validate_location
from pydantic import ValidationError
import json
from fastapi import Depends
from fastapi import HTTPException, status, Request, APIRouter
import motor.motor_asyncio
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.models.ingestion_model import IngestionPayload
from app.config.database import get_database
from app.services.gcs_service import GCSService
import time

settings = get_settings()

router = APIRouter()
security = HTTPBearer()

validate_keys_category = settings.validate_keys_category
validate_values_grouping = settings.validate_values_grouping

async def verify_token_with_client_id(
    authorization: HTTPAuthorizationCredentials = Depends(security)
):
    return await verify_token(authorization)

async def validate_product_data(product):
    invalid_product_ids_variant_gtin_missing = []
    invalid_product_ids_variant_price_missing = []
    invalid_product_ids_variant_gtin_invalid = []
    invalid_product_ids_variant_price_invalid = []

    variants = product.get("variants", [])
    # Validate each variant's GTIN (upc) and price
    if not variants or not isinstance(variants, list):
        invalid_product_ids_variant_gtin_missing.append(product)
        if not variants or not isinstance(variants, list):
            invalid_product_ids_variant_price_missing.append(product)
        return False, invalid_product_ids_variant_gtin_missing, invalid_product_ids_variant_price_missing, invalid_product_ids_variant_gtin_invalid, invalid_product_ids_variant_price_invalid
    variant_invalid = False
    for variant in variants:
        
        variant_gtin = variant.get("upc") or variant.get("gtin")
        variant_price = variant.get("price")

        if variant_gtin is None:
            invalid_product_ids_variant_gtin_missing.append(product)
            variant_invalid = True
            continue
        if not variant_price:
            invalid_product_ids_variant_price_missing.append(product)
            variant_invalid = True
            continue    
        variant_gtin_validation = await validate_gtin(variant_gtin)
        variant_price_validation = validate_price(variant_price)

        if not variant_gtin_validation.get("valid", False):
            invalid_product_ids_variant_gtin_invalid.append(product)
            variant_invalid = True
            continue
        if not variant_price_validation.get("valid", False):
            invalid_product_ids_variant_price_invalid.append(product)
            variant_invalid = True
            continue
        
    if variant_invalid:
        return False, invalid_product_ids_variant_gtin_missing, invalid_product_ids_variant_price_missing, invalid_product_ids_variant_gtin_invalid, invalid_product_ids_variant_price_invalid

    return True, invalid_product_ids_variant_gtin_missing, invalid_product_ids_variant_price_missing, invalid_product_ids_variant_gtin_invalid, invalid_product_ids_variant_price_invalid


def get_errors(error_type,data):
    errors={
        "error":settings.error_messages.get(error_type),
        "data":data
    }
    return errors

def deep_search_values(obj, keys_to_find):
    """
    Recursively search for keys in a deeply nested dict/list structure.
    Returns a dict with keys and list of found values.
    """
    found_values = {key: [] for key in keys_to_find}

    # Explicitly check root-level keys first
    if isinstance(obj, dict):
        for key in keys_to_find:
            if key in obj:
                found_values[key].append(obj[key])

    def _search(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in found_values:
                    found_values[k].append(v)
                _search(v)
        elif isinstance(o, list):
            for item in o:
                _search(item)

    _search(obj)
    return found_values

async def get_keys_to_find(db):
    """
    Fetch keys to find from the validate_keys collection in the database.
    Returns a list of keys.
    """
    keys_cursor = db.validate_keys.find({})
    keys_list = []
    async for key_doc in keys_cursor:
        key = key_doc.get("key_name")
        if key:
            keys_list.append(key)
    return keys_list

@router.post("/JENNiProduct/ProductIngestion", status_code=status.HTTP_200_OK)
async def ingestion_api(
    request: Request,
    payload: IngestionPayload,
    authorized: bool = Depends(verify_token_with_client_id),
    db: motor.motor_asyncio.AsyncIOMotorDatabase = Depends(get_database)
):
    start_time = time.time()
   # validate authorization
    if not authorized:
        raise HTTPException(status_code=401, detail="Invalid Token")

    try:
        ingestion_payload = payload
    except ValidationError as e:
        raise HTTPException(status_code=422, detail="Invalid structure")

    # Validate payload data
    if not ingestion_payload.payload or not isinstance(ingestion_payload.payload, list):
        raise HTTPException(status_code=400, detail="Payload must be a non-empty array of product records")

    # Validate source in db.sources collection
    
    source = ingestion_payload.source
    if not source:
        raise HTTPException(status_code=400, detail="Source field is required in each product record")
    source_exists = await db.sources.find_one({"source_name": source})
    if not source_exists:
        raise HTTPException(status_code=400, detail=f"Source not found:{source}")

    # Validate payload size (max 5MB)
    try:
        data_json = json.dumps(ingestion_payload.payload)
        data_size = len(data_json.encode("utf-8"))
        if data_size > 5 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="Payload exceeds 5MB size limit")
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid payload data format")
    
    
    valid_products = []
    res_messages = []
    res_flag = False
    
    #validate gtin, price and location
    try:
        invalid_product_ids_variant_gtin_missing = []
        invalid_product_ids_variant_price_missing = []
        invalid_product_ids_location_missing = []
        invalid_product_ids_variant_gtin_invalid = []
        invalid_product_ids_variant_price_invalid = []
        invalid_product_ids_location_invalid = []

        valid_products = []

        keys_to_find = list(settings.validate_keys)

        for product in ingestion_payload.payload:
            found_values = deep_search_values(product, keys_to_find)

            # Extract GTIN values from config.json keys
            gtin_keys = validate_keys_category.get("gtin", [])
            gtin_values = []
            for key in gtin_keys:
                gtin_values.extend(found_values.get(key, []))
            # Extract price values from config.json keys
            price_keys = validate_keys_category.get("price", [])
            price_values = []
            for key in price_keys:
                price_values.extend(found_values.get(key, []))
            # Extract location values from config.json keys
            location_keys = validate_keys_category.get("location", [])
            location_values = []
            for key in location_keys:
                location_values.extend(found_values.get(key, []))

            # Validate location presence and correctness
            if not location_values:
                invalid_product_ids_location_missing.append(product)
            else:
                location_str = ""
                # For location, try to get lat,long if available from validate_values_grouping
                lat_keys = []
                long_keys = []
                location_group = validate_values_grouping.get("location", {})
                for group_keys in location_group.values():
                    if len(group_keys) == 2:
                        lat_keys.append(group_keys[0])
                        long_keys.append(group_keys[1])
                lat_vals = []
                long_vals = []
                for lat_key in lat_keys:
                    lat_vals.extend(found_values.get(lat_key, []))
                for long_key in long_keys:
                    long_vals.extend(found_values.get(long_key, []))
                other_vals = []
                for key in location_keys:
                    if key not in lat_keys and key not in long_keys:
                        other_vals.extend(found_values.get(key, []))
                if (lat_vals and long_vals) or other_vals:
                    if lat_vals and long_vals:
                        location_str = f"{lat_vals[0]},{long_vals[0]}"
                    else:
                        location_str = other_vals[0]
                else:
                    invalid_product_ids_location_missing.append(product)

                location_validation = validate_location(location_str)
                if not location_validation.get("valid", False):
                    invalid_product_ids_location_invalid.append(product)

            # Validate GTIN and price for variants
            if "variants" in product and isinstance(product["variants"], list):
                is_valid, invalid_gtin_missing, invalid_price_missing, invalid_gtin_invalid, invalid_price_invalid = await validate_product_data(product)
                if not is_valid:
                    invalid_product_ids_variant_gtin_missing.extend(invalid_gtin_missing)
                    invalid_product_ids_variant_price_missing.extend(invalid_price_missing)
                    invalid_product_ids_variant_gtin_invalid.extend(invalid_gtin_invalid)
                    invalid_product_ids_variant_price_invalid.extend(invalid_price_invalid)
                    continue
                valid_products.append(product)
            elif gtin_values:
                # Validate each GTIN value found
                gtin_valid = True
                for gtin_number in gtin_values:
                    if not isinstance(gtin_number, str):
                        gtin_number = str(gtin_number)
                    if not gtin_number.strip():
                        invalid_product_ids_variant_gtin_missing.append(product)
                        gtin_valid = False
                        
                    validate_gtin_result = validate_gtin(gtin_number)
                    if not validate_gtin_result.get("valid", False):
                        invalid_product_ids_variant_gtin_invalid.append(product)
                        gtin_valid = False
                        
                if not gtin_valid:
                    continue

                # Validate each price value found
                price_valid = True
                for price in price_values:
                    # If price is an object/dict, try to extract numeric value from common keys
                    if isinstance(price, dict):
                        price_num = None
                        for key in ['value', 'amount', 'price']:
                            if key in price:
                                price_num = price[key]
                                break
                        if price_num is None:
                            # If no numeric value found
                            invalid_product_ids_variant_price_invalid.append(product)
                            price_valid = False
                            break
                        price_validation = validate_price(price_num)
                    else:
                        price_validation = validate_price(price)
                    if not price_validation.get("valid", False):
                        invalid_product_ids_variant_price_invalid.append(product)
                        price_valid = False
                        break
                if not price_valid:
                    continue

                valid_products.append(product)
            else:
                # No GTIN found
                invalid_product_ids_variant_gtin_missing.append(product)
                continue

        if invalid_product_ids_variant_gtin_missing or invalid_product_ids_variant_price_missing or invalid_product_ids_location_missing or invalid_product_ids_variant_gtin_invalid or invalid_product_ids_variant_price_invalid or invalid_product_ids_location_invalid:
            if invalid_product_ids_variant_gtin_missing:
                res_messages.append(get_errors("missing_gtin",invalid_product_ids_variant_gtin_missing))
            if invalid_product_ids_variant_price_missing:
                res_messages.append(get_errors("missing_price",invalid_product_ids_variant_price_missing))
            if invalid_product_ids_location_missing:
                res_messages.append(get_errors("missing_location",invalid_product_ids_location_missing))
            if invalid_product_ids_variant_gtin_invalid:
                res_messages.append(get_errors("invalid_gtin",invalid_product_ids_variant_gtin_invalid))  
            if invalid_product_ids_variant_price_invalid:
                res_messages.append(get_errors("invalid_price",invalid_product_ids_variant_price_invalid))
            if invalid_product_ids_location_invalid:
                res_messages.append(get_errors("invalid_location",invalid_product_ids_location_invalid))
            res_flag = True
    except Exception as e:
        raise HTTPException(status_code=400, detail="All products don't have valid gtin, price or location")
       
    
    # Create a new ingestion payload with only valid pr
    # oducts after GTIN check
    ingestion_payload.payload = valid_products
    length_products = len(valid_products)
    
   
    # Send data to pubsub
    try:
        send_to_pubsub(ingestion_payload)
        print('send to pubsub')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    response_message = f"Batch received and {length_products} queued for processing"
    if res_flag:
        # Upload failed records to GCS and return summary + GCS URI
        gcs_service = GCSService(settings.gcs_bucket_name)
        # Transform error data to list of products with aggregated error messages
        product_errors_dict = {}
        product_to_index = {id(prod): idx for idx, prod in enumerate(ingestion_payload.payload)}

        for msg in res_messages:
            error_type = msg["error"]
            # Deduplicate products in msg["data"] by product index
            seen_indices = set()
            for product in msg["data"]:
                product_id = id(product)
                index = product_to_index.get(product_id)
                if index is None:
                    # fallback to string key if product not found
                    index = str(product)
                if index in seen_indices:
                    continue
                seen_indices.add(index)

                if index not in product_errors_dict:
                    product_copy = product.copy()
                    product_copy["error_messages"] = [error_type]
                    product_errors_dict[index] = product_copy
                else:
                    if "error_messages" in product_errors_dict[index] and isinstance(product_errors_dict[index]["error_messages"], list):
                        if error_type not in product_errors_dict[index]["error_messages"]:
                            product_errors_dict[index]["error_messages"].append(error_type)
                    else:
                        product_errors_dict[index]["error_messages"] = [error_type]
        error_data_list = list(product_errors_dict.values())
        gcs_uri_res = gcs_service.upload_json(error_data_list, prefix="errors")
        end_time = time.time()
        print("error_data_list",error_data_list)
        if len(ingestion_payload.payload) > 0 and len(error_data_list) >0:
            return {
                "success": True,
                "valid_count": len(ingestion_payload.payload),
                "invalid_count": len(error_data_list),
                "tracking_id": gcs_uri_res.get("filename"),
                "status_uri": gcs_uri_res.get("uri"),
                "message":f"Successfully processed {len(ingestion_payload.payload)} records. {len(error_data_list)} records had validation issues."
            }
        else:
            raise HTTPException(status_code=400, detail={
                "success": "False",
                "valid_count": len(ingestion_payload.payload),
                "invalid_count": len(error_data_list),
                "tracking_id": gcs_uri_res.get("filename"),
                "status_uri": gcs_uri_res.get("uri"),
                "error": "Payload contains no valid products."
            })
            
    else:
        end_time = time.time()
        return {
            "success": True,
            "valid_count": len(ingestion_payload.payload),
            "message": "Payload processed successfully."
        }
