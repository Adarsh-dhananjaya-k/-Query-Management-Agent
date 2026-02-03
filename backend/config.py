# config.py
import os
import httpx
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()

def get_azure_client():
    """
    Create Azure OpenAI client with a custom HTTP client to avoid the 'proxies' bug 
    seen in some environments where system proxies are auto-injected.
    """
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key  = os.getenv("AZURE_OPENAI_API_KEY")
    version  = os.getenv("AZURE_OPENAI_API_VERSION")

    if not endpoint or not api_key or not version:
        raise ValueError("Missing required env variables for Azure OpenAI.")

    # Using a custom httpx client avoids the internal 'proxies' keyword issue
    # by taking control of the transport layer.
    http_client = httpx.Client(
        verify=True,
    )

    return AzureOpenAI(
        azure_endpoint = endpoint,
        api_key        = api_key,
        api_version    = version,
        http_client    = http_client
    )


def get_deployment_name():
    name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
    if not name:
        raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME is not set in .env")
    return name


# Quick debug helper (run this file directly to test)
if __name__ == "__main__":
    try:
        client = get_azure_client()
        print("AzureOpenAI client initialized successfully with custom http_client.")
        print("Deployment name:", get_deployment_name())
    except Exception as e:
        print("Error:", str(e))
