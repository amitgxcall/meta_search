import unittest
import os
import tempfile
from job_search import UnifiedJobSearch, FieldMapping

class TestUnifiedSearch(unittest.TestCase):
    
    def setUp(self):
        # Create a temporary CSV file
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        self.temp_file.write(b"""job_id,job_name,status,description,created_at,duration_minutes
1,database_backup,success,Daily backup of database,2023-01-01 00:00:00,10
2,data_processing,failed,Process customer data,2023-01-02 00:00:00,20
3,security_scan,running,Scan for vulnerabilities,2023-01-03 00:00:00,30
4,database_cleanup,success,Clean old records,2023-01-04 00:00:00,15
5,etl_pipeline,failed,Data pipeline failure,2023-01-05 00:00:00,25
""")
        self.temp_file.close()
        
        # Create the unified search interface
        self.search = UnifiedJobSearch(self.temp_file.name)
    
    def tearDown(self):
        # Remove temporary file
        os.unlink(self.temp_file.name)
    
    def test_search(self):
        # Test basic search
        results = self.search.search("database")
        self.assertGreaterEqual(len(results), 1)
        
        # Test field-specific search
        results = self.search.search("status:failed")
        self.assertEqual(len(results), 2)
    
    def test_get_record_by_id(self):
        record = self.search.get_record_by_id("1")
        self.assertIsNotNone(record)
        self.assertEqual(record["name"], "database_backup")
    
    def test_custom_field_mapping(self):
        # Create custom field mapping
        field_mapping = FieldMapping(
            id_field="job_id",
            name_field="job_name",
            status_field="status"
        )
        
        # Create new search with custom mapping
        custom_search = UnifiedJobSearch(self.temp_file.name, field_mapping=field_mapping)
        
        # Test with custom mapping
        record = custom_search.get_record_by_id("1")
        self.assertEqual(record["id"], "1")
        self.assertEqual(record["name"], "database_backup")

if __name__ == '__main__':
    unittest.main()