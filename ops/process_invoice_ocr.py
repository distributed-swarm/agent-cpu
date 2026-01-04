import pytesseract
from pdf2image import convert_from_path
import re
import os

def run(payload):
    # Validates input immediately
    pdf_path = payload.get("pdf_path")
    if not pdf_path:
        return {"error": "No pdf_path provided"}
    
    # Safety Check: Does the file actually exist?
    if not os.path.exists(pdf_path):
        return {"error": f"File not found at: {pdf_path}"}

    try:
        images = convert_from_path(pdf_path)
        full_text = ""
        
        for i, img in enumerate(images):
            # Adds page markers so you know which page text came from
            full_text += f"\n--- Page {i+1} ---\n" + pytesseract.image_to_string(img)
            
        data = {
            # Improved regex to capture dashes in invoice numbers (e.g., INV-2024)
            "invoice_number": extract_field(r'Invoice\s*#?\s*[:.]?\s*([A-Za-z0-9-]+)', full_text),
            "date": extract_field(r'Date\s*[:.]?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', full_text),
            "total_amount": extract_field(r'Total\s*[:.]?\s*\$?\s*([\d,]+\.\d{2})', full_text),
            "vendor": extract_vendor(full_text),
            "page_count": len(images),
            "raw_text_snippet": full_text[:200] + "..." # Returns start of text for easy checking
        }
        return data

    except Exception as e:
        return {"error": f"OCR Failed: {str(e)}"}

def extract_field(pattern, text):
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1) if match else None

def extract_vendor(text):
    text_lower = text.lower()
    if "amazon" in text_lower: return "Amazon Web Services"
    if "oracle" in text_lower: return "Oracle Corp"
    if "sap" in text_lower: return "SAP SE"
    if "google" in text_lower: return "Google Cloud"
    return "Unknown Vendor"
