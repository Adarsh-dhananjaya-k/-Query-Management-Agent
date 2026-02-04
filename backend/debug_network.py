import httpx
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    print(f"Testing connection to: {endpoint}")
    
    try:
        # Test basic connectivity to the endpoint
        with httpx.Client(verify=True) as client:
            resp = client.get(endpoint)
            print(f"Response status: {resp.status_code}")
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        
        print("\nChecking for environment proxies...")
        for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
            val = os.environ.get(key)
            if val:
                print(f"{key}: {val}")

if __name__ == "__main__":
    test_connection()
