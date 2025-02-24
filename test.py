import unittest
import requests

class ExtractGdriveTestCase(unittest.TestCase):
    def test_health_check(self):
        url = "http://localhost:8080/health_check"
        headers = {"Content-Type": "application/json"}
        payload = {}

        response = requests.request("GET", url, headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "Health Check: Alive")

    def test_err_no_payload(self):
        url = "http://localhost:8080"
        headers = {"Content-Type": "application/json"}
        payload = {}

        response = requests.request("POST", url, headers=headers, json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.text, "No Payload")
    
    def test_the1(self):
        url = "http://localhost:8080"
        headers = {"Content-Type": "application/json"}
        payload = {"areas":["the1"]}

        response = requests.request("POST", url, headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "??????")
    
    def test_nielsen(self):
        url = "http://localhost:8080"
        headers = {"Content-Type": "application/json"}
        payload = {"areas":["nielsen"]}

        response = requests.request("POST", url, headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "??????")
    
    def test_budget_target(self):
        url = "http://localhost:8080"
        headers = {"Content-Type": "application/json"}
        payload = {"areas":["budget_target"]}

        response = requests.request("POST", url, headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "??????")
    
    def test_social_listening(self):
        url = "http://localhost:8080"
        headers = {"Content-Type": "application/json"}
        payload = {"areas":["social_listening"]}

        response = requests.request("POST", url, headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "??????")
    
    def test_2area(self):
        url = "http://localhost:8080"
        headers = {"Content-Type": "application/json"}
        payload = {"areas":["nielsen","social_listening"]}

        response = requests.request("POST", url, headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "??????")

if __name__ == '__main__':
    unittest.main()