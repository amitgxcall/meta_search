import unittest
import os
import tempfile
from meta_search.providers.csv_provider import CSVProvider
from meta_search.utils.field_mapping import FieldMapping

class TestCSVProvider(unittest.TestCase):
    
    def setUp(self):
        # Create a temporary CSV file
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        self.temp_file.write(b"""job_id,job_name,status,created_at,duration_minutes
1,job1,success,2023-01-01 00:00:00,10
2,job2,failed,2023-01-02 00:00:00,20
3,job3,running,2023-01-03 00:00:00,30
""")
        self.temp_file.close()
        
        # Create field mapping
        self.field_mapping = FieldMapping(
            id_field="job_id",
            name_field="job_name",
            status_field="status",
            timestamp_fields=["created_at"],
            numeric_fields=["duration_minutes"]
        )
        
        # Create provider
        self.provider = CSVProvider(self.temp_file.name, self.field_mapping)
    
    def tearDown(self):
        # Remove temporary file
        os.unlink(self.temp_file.name)
    
    def test_get_all_fields(self):
        fields = self.provider.get_all_fields()
        self.assertEqual(len(fields), 5)
        self.assertIn("job_id", fields)
        self.assertIn("job_name", fields)
        self.assertIn("status", fields)
        self.assertIn("created_at", fields)
        self.assertIn("duration_minutes", fields)
    
    def test_get_record_count(self):
        count = self.provider.get_record_count()
        self.assertEqual(count, 3)
    
    def test_get_all_records(self):
        records = self.provider.get_all_records()
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["job_name"], "job1")
        self.assertEqual(records[1]["job_name"], "job2")
        self.assertEqual(records[2]["job_name"], "job3")
    
    def test_get_record_by_id(self):
        record = self.provider.get_record_by_id("2")
        self.assertIsNotNone(record)
        self.assertEqual(record["job_name"], "job2")
        self.assertEqual(record["status"], "failed")
    
    def test_query_records(self):
        # Test exact match
        results = self.provider.query_records({"status": "failed"})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["job_name"], "job2")
        
        # Test comparison
        results = self.provider.query_records({"duration_minutes": {"gt": 15}})
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["job_name"], "job2")
        self.assertEqual(results[1]["job_name"], "job3")
    
    def test_get_text_for_vector_search(self):
        record = self.provider.get_record_by_id("1")
        field_weights = {"job_name": 2.0, "status": 1.0}
        text = self.provider.get_text_for_vector_search(record, field_weights)
        self.assertIn("job1", text)
        self.assertIn("success", text)

if __name__ == '__main__':
    unittest.main()