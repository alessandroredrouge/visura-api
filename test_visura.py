"""
Test script for the visura-api server.
Parses cadastral parcel main_source_ids, submits visura requests, and polls for results.

Usage:
    1. Start the server:  uvicorn main:app --host 0.0.0.0 --port 8020
    2. Run this script:   python test_visura.py

Requires the server to be running and authenticated with SISTER.
"""

import time
import json
import requests

BASE_URL = "http://localhost:8020"
POLL_INTERVAL = 5   # seconds between polls
POLL_TIMEOUT = 120  # max seconds to wait per request


# --- Test parcels ---
# Each entry: (provincia, comune, foglio, particella, sezione_or_None)
TEST_PARCELS = [
    ("Savona", "GIUSTENICE", "18", "267", None),
]


def check_health() -> bool:
    """Check if the server is running and authenticated."""
    try:
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        data = resp.json()
        print(f"Health: {data}")
        if not data.get("authenticated"):
            print("WARNING: Server is not authenticated with SISTER yet.")
            return False
        return True
    except requests.ConnectionError:
        print(f"ERROR: Cannot connect to server at {BASE_URL}. Is it running?")
        return False


def submit_visura(provincia: str, comune: str, foglio: str, particella: str,
                  sezione: str = None, tipo_catasto: str = None) -> list[str]:
    """Submit a visura request. Returns list of request_ids."""
    payload = {
        "provincia": provincia,
        "comune": comune,
        "foglio": foglio,
        "particella": particella,
    }
    if tipo_catasto:
        payload["tipo_catasto"] = tipo_catasto
    if sezione:
        payload["sezione"] = sezione

    print(f"\n--- Submitting visura ---")
    print(f"  Payload: {json.dumps(payload)}")

    resp = requests.post(f"{BASE_URL}/visura", json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    print(f"  Status: {data['status']}")
    print(f"  Request IDs: {data['request_ids']}")
    print(f"  Message: {data['message']}")

    return data["request_ids"]


def submit_intestati(provincia: str, comune: str, foglio: str, particella: str,
                     tipo_catasto: str, subalterno: str, sezione: str = None) -> str:
    """Submit an intestati (owners) request for a specific unit. Returns request_id."""
    payload = {
        "provincia": provincia,
        "comune": comune,
        "foglio": foglio,
        "particella": particella,
        "tipo_catasto": tipo_catasto,
        "subalterno": subalterno,
    }
    if sezione:
        payload["sezione"] = sezione

    print(f"\n--- Submitting intestati request ---")
    print(f"  Payload: {json.dumps(payload)}")

    resp = requests.post(f"{BASE_URL}/visura/intestati", json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    print(f"  Status: {data['status']}")
    print(f"  Request ID: {data['request_id']}")
    print(f"  Message: {data['message']}")

    return data["request_id"]


def poll_result(request_id: str) -> dict | None:
    """Poll for a visura result until completed or timeout."""
    print(f"\n--- Polling {request_id} ---")
    start = time.time()

    while time.time() - start < POLL_TIMEOUT:
        resp = requests.get(f"{BASE_URL}/visura/{request_id}", timeout=10)
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status")
        if status == "completed":
            print(f"  Completed in {time.time() - start:.1f}s")
            return data
        elif status == "error":
            print(f"  ERROR: {data.get('error')}")
            return data
        else:
            elapsed = time.time() - start
            print(f"  Still processing... ({elapsed:.0f}s)", end="\r")
            time.sleep(POLL_INTERVAL)

    print(f"\n  TIMEOUT after {POLL_TIMEOUT}s")
    return None


def extract_subalterni(visura_result: dict) -> list[str]:
    """Extract subalterno values from a Fabbricati visura result."""
    if not visura_result or visura_result.get("status") != "completed":
        return []

    subalterni = []
    results = visura_result.get("data", {})

    # Check immobili list for Sub/Subalterno field
    for imm in results.get("immobili", []):
        sub = imm.get("Sub") or imm.get("Subalterno") or imm.get("sub") or imm.get("subalterno")
        if sub and sub.strip():
            subalterni.append(sub.strip())

    # Also check detailed results
    if not subalterni:
        for res in results.get("results", []):
            immobile = res.get("immobile", {})
            sub = immobile.get("Sub") or immobile.get("Subalterno") or immobile.get("sub") or immobile.get("subalterno")
            if sub and sub.strip():
                subalterni.append(sub.strip())

    return subalterni


def print_results(data: dict):
    """Pretty-print visura results."""
    if not data or data.get("status") != "completed":
        return

    results = data.get("data", {})
    immobili = results.get("immobili", [])
    intestati = results.get("intestati", [])
    all_results = results.get("results", [])

    print(f"\n  Tipo catasto: {data.get('tipo_catasto')}")
    print(f"  Immobili found: {len(immobili)}")

    for i, imm in enumerate(immobili):
        print(f"    [{i+1}] {json.dumps(imm, ensure_ascii=False)}")

    if all_results:
        print(f"  Detailed results: {len(all_results)}")
        for i, res in enumerate(all_results):
            res_intestati = res.get("intestati", [])
            print(f"    Result [{i+1}]: {len(res_intestati)} intestati")
            for intestato in res_intestati:
                print(f"      - {json.dumps(intestato, ensure_ascii=False)}")

    if intestati:
        print(f"  Top-level intestati: {len(intestati)}")
        for intestato in intestati:
            print(f"    - {json.dumps(intestato, ensure_ascii=False)}")


def test_parcel(provincia: str, comune: str, foglio: str, particella: str, sezione: str = None):
    """Run a full visura test for a single parcel."""
    print(f"\n{'='*70}")
    print(f"TESTING: {provincia} / {comune} / Foglio {foglio} / Particella {particella}")
    print(f"{'='*70}")

    # Submit (queries both T and F when tipo_catasto is omitted)
    request_ids = submit_visura(
        provincia=provincia,
        comune=comune,
        foglio=foglio,
        particella=particella,
        sezione=sezione,
    )

    # Poll each request and collect Fabbricati results for owner lookup
    for req_id in request_ids:
        result = poll_result(req_id)
        print_results(result)

        # For Fabbricati: the initial visura does NOT extract owners.
        # We need a separate /visura/intestati call per subalterno.
        if result and result.get("tipo_catasto") == "F" and result.get("status") == "completed":
            subalterni = extract_subalterni(result)
            if subalterni:
                print(f"\n  Found {len(subalterni)} subalterni in Fabbricati: {subalterni}")
                print(f"  Fetching owners for each...")

                for sub in subalterni:
                    intestati_req_id = submit_intestati(
                        provincia=provincia,
                        comune=comune,
                        foglio=foglio,
                        particella=particella,
                        tipo_catasto="F",
                        subalterno=sub,
                        sezione=sezione,
                    )
                    intestati_result = poll_result(intestati_req_id)
                    if intestati_result and intestati_result.get("status") == "completed":
                        print(f"\n  Owners for subalterno {sub}:")
                        print_results(intestati_result)
                    else:
                        print(f"\n  No owner data for subalterno {sub}")
            else:
                print(f"\n  No subalterni found in Fabbricati result — cannot fetch owners")


def main():
    print("Visura API Test Script")
    print(f"Server: {BASE_URL}")
    print()

    if not check_health():
        return

    for provincia, comune, foglio, particella, sezione in TEST_PARCELS:
        test_parcel(provincia, comune, foglio, particella, sezione)

    print(f"\n{'='*70}")
    print("All tests completed.")


if __name__ == "__main__":
    main()
