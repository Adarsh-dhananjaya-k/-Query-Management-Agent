import httpx
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    print(f"Testing connection to: {endpoint}")
    
    # Try with system proxies (default)
    try:
        print("\n--- Try 1: Default (System Proxies) ---")
        with httpx.Client(verify=True, timeout=10.0) as client:
            resp = client.get(endpoint)
            print(f"Success! Status: {resp.status_code}")
    except Exception as e:
        print(f"Failed default: {str(e)}")

    # Try WITHOUT system proxies
    try:
        print("\n--- Try 2: Ignoring System Proxies ---")
        with httpx.Client(verify=True, trust_env=False, timeout=10.0) as client:
            resp = client.get(endpoint)
            print(f"Success! Status: {resp.status_code}")
    except Exception as e:
        print(f"Failed ignoring proxies: {str(e)}")

    # Try reaching a common site
    try:
        print("\n--- Try 3: Reaching google.com ---")
        with httpx.Client(verify=True, timeout=5.0) as client:
            resp = client.get("https://www.google.com")
            print(f"Google Reachable: {resp.status_code}")
    except Exception as e:
        print(f"Google Unreachable: {str(e)}")

if __name__ == "__main__":
    test_connection()
