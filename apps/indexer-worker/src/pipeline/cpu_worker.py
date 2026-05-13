from ecom_image.engine import ImageEngine
import logging

# We initialize a single engine per process for efficiency
_engine = ImageEngine()

def process_image_cpu_task(img_bytes: bytes) -> dict:
    """
    This function runs in a separate process.
    It takes raw bytes and returns resized JPEG bytes (original and thumbnail).
    """
    try:
        # The heavy PIL math happens here
        return _engine.process(img_bytes)
    except Exception as e:
        # We catch and return error strings because exceptions 
        # sometimes don't pickle well across process boundaries
        return {"error": str(e)}
