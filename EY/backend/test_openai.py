from openai import AzureOpenAI
import os
import sys

print("Python Version:", sys.version)
try:
    import openai
    print("OpenAI Version:", openai.__version__)
except Exception as e:
    print("Could not get version:", e)

try:
    client = AzureOpenAI(
        azure_endpoint="https://test.com",
        api_key="test",
        api_version="2023-05-15"
    )
    print("Client initialized successfully with test values.")
except Exception as e:
    print("Error during initialization:")
    import traceback
    traceback.print_exc()
