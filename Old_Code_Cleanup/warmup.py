import requests
import time
import datetime

# Define the URLs of your application endpoints to warm up
endpoints = [
    "http://maxsipsupport.com/"
    # Add more endpoints as needed
]

# Define the interval between warm-up requests (in seconds)
warmup_interval = 6  # 5 minutes

def warm_up():
    for endpoint in endpoints:
        try:
            response = requests.get(endpoint, timeout=10)
            if response.status_code == 200:
                print(f"Warmed up: {endpoint} - Time now is: {datetime.datetime.now()}")
            else:
                print(f"Failed to warm up: {endpoint} - Status code: {response.status_code}")
        except Exception as e:
            print(f"Failed to warm up: {endpoint} - Error: {str(e)}")

if __name__ == "__main__":
    while True:
        warm_up()
        time.sleep(warmup_interval)