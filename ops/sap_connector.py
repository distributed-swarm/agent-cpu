import time
import requests
import shutil
import csv
import os
from pathlib import Path

# Configuration
LEASE_REAPER_URL = "http://localhost:8080"  # Your Controller
WATCH_DIR = Path("./sap_exports/pending")
PROCESSED_DIR = Path("./sap_exports/processed")
RESULTS_FILE = Path("./sap_exports/import_ready.csv")

# Ensure dirs exist
WATCH_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

print(f"[*] Connector Active. Watching {WATCH_DIR}...")

def write_to_erp_csv(data, filename):
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='') as csvfile:
        fieldnames = ['invoice_number', 'date', 'total_amount', 'vendor', 'source_file']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()
        
        writer.writerow(data)

while True:
    # 1. Scan for new PDFs
    for pdf_file in WATCH_DIR.glob("*.pdf"):
        print(f"\n[+] Found Invoice: {pdf_file.name}")
        
        try:
            # 2. Submit Job to Swarm
            # Note: For this to work, the AGENT needs access to the file path, 
            # OR you need to upload the file content. 
            # For simplicity here, we assume a shared network mount (/mnt/erp).
            job_payload = {
                "op": "process_invoice_ocr",
                "payload": {"pdf_path": str(pdf_file.absolute())}
            }
            
            resp = requests.post(f"{LEASE_REAPER_URL}/api/job", json=job_payload)
            if resp.status_code != 200:
                print(f"[-] Failed to submit job: {resp.text}")
                continue
                
            job_id = resp.json()['job_ids'][0]
            print(f"    -> Job Submitted ({job_id}). Waiting for GPU/TPU...")

            # 3. Poll for Result (Simple Polling)
            status = "queued"
            while status not in ["completed", "failed"]:
                time.sleep(1)
                check = requests.get(f"{LEASE_REAPER_URL}/api/jobs/{job_id}")
                status = check.json()['state']
            
            result = check.json().get('result', {})
            
            if status == "failed":
                print(f"    -> Job Failed: {check.json().get('error')}")
                continue

            # 4. Success! Write to ERP CSV
            print(f"    -> OCR Complete: {result.get('total_amount')} from {result.get('vendor')}")
            
            csv_row = result
            csv_row['source_file'] = pdf_file.name
            write_to_erp_csv(csv_row, RESULTS_FILE)
            
            # 5. Archive the PDF
            shutil.move(str(pdf_file), str(PROCESSED_DIR / pdf_file.name))
            print("    -> Archived & Saved to CSV.")

        except Exception as e:
            print(f"[-] Critical Error processing {pdf_file.name}: {e}")

    time.sleep(5)
