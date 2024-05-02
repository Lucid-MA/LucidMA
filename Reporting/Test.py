import requests

# Constants
API_BASE_URL = 'https://api.foreupsoftware.com/api_rest/index.php/'
AUTH_ENDPOINT = API_BASE_URL + 'tokens'
BOOKING_ENDPOINT = API_BASE_URL + 'booking_endpoint'  # Replace with actual endpoint for booking
USERNAME = 'tonyus137@gmail.com'
PASSWORD = '123456Abc'
DATE = '2024-05-11'
TIME = '08:20'

def get_auth_token(email, password):
    payload = {'email': email, 'password': password}
    response = requests.post(AUTH_ENDPOINT, json=payload)
    response_data = response.json()
    return response_data['token']  # Adjust according to actual API response structure

def book_tee_time(token, date, time):
    headers = {'Authorization': f'Bearer {token}'}
    payload = {'date': date, 'time': time}  # Adjust payload based on actual API requirements
    response = requests.post(BOOKING_ENDPOINT, headers=headers, json=payload)
    return response.status_code == 200

def main():
    token = get_auth_token(USERNAME, PASSWORD)
    if book_tee_time(token, DATE, TIME):
        print("Tee time booked successfully!")
    else:
        print("Failed to book tee time.")

if __name__ == '__main__':
    main()