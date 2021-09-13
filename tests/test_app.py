import unittest
from unittest import mock

# import flaskr

from lambda_functions.csv_processor import processor


class TestCsvProcessor(unittest.TestCase):
    def test_extract_csv_data_good(self):
        data_csv_path = "sample-csvs/sample-good.csv"

        expected = {
            "rows": [
                {
                    "customer_id": "123456",
                    "name": "Tom",
                    "age": "34",
                    "date_of_birth": "17-04-1987",
                    "favourite_colour": "Green",
                },
                {
                    "customer_id": "234567",
                    "name": "Michael",
                    "age": "20",
                    "date_of_birth": "01-01-2001",
                    "favourite_colour": "Blue",
                },
                {
                    "customer_id": "345678",
                    "name": "Marie",
                    "age": "29",
                    "date_of_birth": "12-09-1992",
                    "favourite_colour": "Purple",
                },
                {
                    "customer_id": "456789",
                    "name": "Ivan",
                    "age": "66",
                    "date_of_birth": "13-08-1955",
                    "favourite_colour": "Red",
                },
                {
                    "customer_id": "567890",
                    "name": "Pablo",
                    "age": "45",
                    "date_of_birth": "01-02-1976",
                    "favourite_colour": "Gold",
                },
            ]
        }

        response = processor.extract_csv_data(data_csv_path)

        self.assertEqual(response, expected)

    def test_extract_csv_data_bad(self):
        data_csv_path = "sample-csvs/sample-bad.csv"

        expected = {"rows": []}

        with self.assertLogs(level="INFO") as logs:
            response = processor.extract_csv_data(data_csv_path)
            self.assertEqual(response, expected)
            self.assertEqual(len(logs.output), 5)
            self.assertIn("customer_id not present", logs.output[0])
            self.assertIn("customer_id not present", logs.output[1])
            self.assertIn("customer_id not present", logs.output[2])
            self.assertIn("customer_id not present", logs.output[3])
            self.assertIn("customer_id not present", logs.output[4])

    def test_transform_csv_data(self):
        data_csv_data = {
            "rows": [
                {
                    "customer_id": "123456",
                    "name": "Tom",
                    "age": "34",
                    "date_of_birth": "17-04-1987",
                    "favourite_colour": "Green",
                },
                {
                    "customer_id": "234567",
                    "name": "Michael",
                    "age": "20",
                    "date_of_birth": "01-01-2001",
                    "favourite_colour": "Blue",
                },
            ]
        }

        expected = [
            {
                "customer_id": {"S": "123456"},
                "name": {"S": "Tom"},
                "age": {"S": "34"},
                "date_of_birth": {"S": "17-04-1987"},
                "favourite_colour": {"S": "Green"},
            },
            {
                "customer_id": {"S": "234567"},
                "name": {"S": "Michael"},
                "age": {"S": "20"},
                "date_of_birth": {"S": "01-01-2001"},
                "favourite_colour": {"S": "Blue"},
            },
        ]

        response = processor.transform_csv_data(data_csv_data)

        self.assertEqual(response, expected)
