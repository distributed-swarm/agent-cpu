# ops/process_invoice_ocr.py
import pytesseract
from pdf2image import convert_from_path
import re
import os

# Ensure Tesseract is installed in the container:
# RUN apt-get install -y tesseract-ocr poppler-utils

def run(payload):
    """
    Payload: {"pdf_path": "/mnt/data/invoice_123.pdf"} (Agent must have access to this path)
    OR
    Payload: {"image_url": "http://..."} if sending files over HTTP
    """
    pdf_path = payload.get("pdf_path")
    
    try:
        # 1. Convert PDF pages to images
        images = convert_from_path(pdf_path)
        full_text = ""
        
        # 2. Run OCR on each page
        for img in images:
            full_text += pytesseract.image_to_string(img)
            
        # 3. "Smart" Extraction (Basic Regex for Demo)
        # In production, use your BART model here for "Named Entity Recognition"
        data = {
            "invoice_number": extract_field(r'Invoice\s*#?\s*[:.]?\s*(\w+)', full_text),
            "date": extract_field(r'Date\s*[:.]?\s*(\d{2}[/-]\d{2}[/-]\d{2,4})', full_text),
            "total_amount": extract_field(r'Total\s*[:.]?\s*\$?\s*([\d,]+\.\d{2})', full_text),
            "vendor": extract_vendor(full_text),
            "raw_text_length": len(full_text)
        }
        
        return data

    except Exception as e:
        return {"error": str(e)}

def extract_field(pattern, text):
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1) if match else None

def extract_vendor(text):
    # Simple keyword matching for demo
    if "Amazon" in text: return "Amazon Web Services"
    if "Oracle" in text: return "Oracle Corp"
    return "Unknown Vendor"
