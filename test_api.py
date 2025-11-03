from sodapy import Socrata

# Test the Socrata client
client = Socrata("data.cityofnewyork.us", None)

print("Testing Socrata client...")
try:
    results = client.get("5zhs-2jue", limit=5)
    print(f"Success! Got {len(results)} records")
    print(f"First record keys: {results[0].keys() if results else 'No data'}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
finally:
    client.close()
