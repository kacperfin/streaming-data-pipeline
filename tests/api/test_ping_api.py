from api.ping_api import ping_api

# Real API call
def test_ping_api():
    status_code, response_json = ping_api()

    expected_response = {
        'gecko_says': '(V3) To the Moon!'
    }

    assert status_code == 200
    assert response_json == expected_response