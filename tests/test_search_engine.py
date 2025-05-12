import unittest
import os
import tempfile
from meta_search.providers.csv_provider import CSVProvider
from meta_search.utils.field_mapping import FieldMapping
from meta_search.search.engine import SearchEngine

class TestSearchEngine(unittest.TestCase):
    
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
        
        # Create field mapping
        self.field_mapping = FieldMapping(
            id_field="job_id",
            name_field="job_name",
            status_field="status",
            timestamp_fields=["created_at"],
            numeric_fields=["duration_minutes"],
            text_fields=["description"]
        )
        
        # Create provider and search engine
        self.provider = CSVProvider(self.temp_file.name, self.field_mapping)
        self.search_engine = SearchEngine(self.provider)
    
    def tearDown(self):
        # Remove temporary file
        os.unlink(self.temp_file.name)
    
    def test_extract_filters(self):
        # Test field:value extraction
        filters = self.search_engine.extract_filters("status:failed")
        self.assertEqual(filters, {"status": "failed"})
        
        # Test comparison extraction
        filters = self.search_engine.extract_filters("duration_minutes>15")
        self.assertEqual(filters["duration_minutes"]["gt"], 15)
    
    def test_search(self):
        # Test simple search
        results = self.search_engine.search("database")
        self.assertGreaterEqual(len(results), 1)
        
        # Test structured search
        results = self.search_engine.search("status:failed")
        self.assertEqual(len(results), 2)
        
        # Test combined search
        results = self.search_engine.search("database status:success")
        for result in results:
            self.assertEqual(result["job_details"]["status"], "success")
            self.assertIn("database", result["job_details"]["job_name"].lower())
    
    def test_format_for_llm(self):
        results = self.search_engine.search("database")
        llm_format = self.search_engine.format_for_llm(results, "database")
        
        self.assertIn("query", llm_format)
        self.assertIn("results", llm_format)
        self.assertIn("suggested_response", llm_format)

if __name__ == '__main__':
    unittest.main()