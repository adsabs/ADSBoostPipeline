#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import json
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from adsboost.tasks import (
    task_process_boost_request_message,
    task_compute_boost_factors,
    task_query_boost_factors,
    task_export_boost_factors,
    task_store_boost_factors,
    task_send_to_master_pipeline
)
from adsboost.app import ADSBoostCelery
from adsputils import load_config, setup_logging

# ============================= INITIALIZATION ==================================== #
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)

logger = setup_logging('test_tasks.py', proj_home=proj_home,
                       level=config.get('LOGGING_LEVEL', 'DEBUG'),
                       attach_stdout=config.get('LOG_STDOUT', False))

class TestTasks:
    """Test all functionality of tasks.py"""
    
    @pytest.fixture
    def app(self):
        """Create app instance for testing"""
        app = ADSBoostCelery('ADSBoostPipeline')
        return app
    
    @pytest.fixture
    def sample_record(self):
        """Sample record for testing"""
        return {
            "bibcode": "2022ApJ...931...44P",
            "scix_id": "scix:75M6-3WST-4DM1",
            "bib_data": {
                "abstract": "This is a test abstract",
                "author": ["Test Author"],
                "bibcode": "2022ApJ...931...44P",
                "database": ["astronomy"],
                "date": "2022-01-01T00:00:00.000000Z",
                "doctype": "article",
                "pubdate": "2022-01-01",
                "title": ["Test Title"],
                "year": "2022",
                "entry_date": "2022-01-15T00:00:00.000000Z"
            },
            "metrics": {
                "bibcode": "2022ApJ...931...44P",
                "refereed": True,
                "citation_num": 5,
                "status": "active"
            },
            "classifications": {
                "database": ["astronomy"]
            }
        }
    
    @pytest.fixture
    def sample_boost_factors(self):
        """Sample boost factors for testing"""
        return {
            'doctype_boost': 1.0,
            'refereed_boost': 1.0,
            'recency_boost': 0.8,
            'boost_factor': 0.933,
            'astrophysics_weight': 1.0,
            'physics_weight': 0.64,
            'earthscience_weight': 0.1,
            'planetary_weight': 0.46,
            'heliophysics_weight': 0.28,
            'general_weight': 0.64,
            'astrophysics_final_boost': 0.933,
            'physics_final_boost': 0.597,
            'earthscience_final_boost': 0.093,
            'planetary_final_boost': 0.429,
            'heliophysics_final_boost': 0.261,
            'general_final_boost': 0.597
        }
    
    def test_task_process_boost_request_message_success(self, sample_record):
        """Test successful processing of boost request message"""
        message = json.dumps(sample_record)
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.handle_message_payload.return_value = None
            
            result = task_process_boost_request_message(message)
            
            assert result == "success"
            mock_app.handle_message_payload.assert_called_once_with(message=message)
    
    def test_task_process_boost_request_message_error(self, sample_record):
        """Test error handling in boost request message processing"""
        message = json.dumps(sample_record)
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.handle_message_payload.side_effect = Exception("Test error")
            
            with pytest.raises(Exception, match="Test error"):
                task_process_boost_request_message(message)
    
    def test_task_compute_boost_factors_success(self, sample_record, sample_boost_factors):
        """Test successful computation of boost factors"""
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.compute_final_boost.return_value = sample_boost_factors
            
            result = task_compute_boost_factors(sample_record)
            
            assert result == sample_boost_factors
            mock_app.compute_final_boost.assert_called_once_with(sample_record)
    
    def test_task_compute_boost_factors_error(self, sample_record):
        """Test error handling in boost factor computation"""
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.compute_final_boost.side_effect = Exception("Computation error")
            
            with pytest.raises(Exception, match="Computation error"):
                task_compute_boost_factors(sample_record)
    
    def test_task_store_boost_factors_success(self, sample_boost_factors):
        """Test successful storage of boost factors"""
        bibcode = "2022ApJ...931...44P"
        scix_id = "scix:75M6-3WST-4DM1"
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.store_boost_factors.return_value = None
            
            result = task_store_boost_factors(bibcode, scix_id, sample_boost_factors)
            
            assert result == "success"
            mock_app.store_boost_factors.assert_called_once_with(bibcode, scix_id, sample_boost_factors)
    
    def test_task_store_boost_factors_error(self, sample_boost_factors):
        """Test error handling in boost factor storage"""
        bibcode = "2022ApJ...931...44P"
        scix_id = "scix:75M6-3WST-4DM1"
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.store_boost_factors.side_effect = Exception("Storage error")
            
            with pytest.raises(Exception, match="Storage error"):
                task_store_boost_factors(bibcode, scix_id, sample_boost_factors)
    
    def test_task_send_to_master_pipeline_success(self, sample_record, sample_boost_factors):
        """Test successful sending to master pipeline"""
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.send_to_master_pipeline.return_value = None
            
            result = task_send_to_master_pipeline(sample_record, sample_boost_factors)
            
            assert result == "success"
            mock_app.send_to_master_pipeline.assert_called_once_with(sample_record, sample_boost_factors)
    
    def test_task_send_to_master_pipeline_error(self, sample_record, sample_boost_factors):
        """Test error handling in sending to master pipeline"""
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.send_to_master_pipeline.side_effect = Exception("Send error")
            
            with pytest.raises(Exception, match="Send error"):
                task_send_to_master_pipeline(sample_record, sample_boost_factors)
    
    def test_task_query_boost_factors_by_bibcode(self):
        """Test querying boost factors by bibcode"""
        bibcode = "2022ApJ...931...44P"
        expected_results = [{"bibcode": bibcode, "doctype_boost": 1.0}]
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.query_boost_factors.return_value = expected_results
            
            result = task_query_boost_factors(bibcode=bibcode)
            
            assert result == expected_results
            mock_app.query_boost_factors.assert_called_once_with(bibcode=bibcode, scix_id=None)
    
    def test_task_query_boost_factors_by_scix_id(self):
        """Test querying boost factors by scix_id"""
        scix_id = "scix:75M6-3WST-4DM1"
        expected_results = [{"scix_id": scix_id, "doctype_boost": 1.0}]
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.query_boost_factors.return_value = expected_results
            
            result = task_query_boost_factors(scix_id=scix_id)
            
            assert result == expected_results
            mock_app.query_boost_factors.assert_called_once_with(bibcode=None, scix_id=scix_id)
    
    def test_task_query_boost_factors_error(self):
        """Test error handling in boost factor querying"""
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.query_boost_factors.side_effect = Exception("Query error")
            
            with pytest.raises(Exception, match="Query error"):
                task_query_boost_factors(bibcode="test")
    
    def test_task_export_boost_factors_success(self, sample_boost_factors):
        """Test successful export of boost factors"""
        output_path = "/tmp/test_export.csv"
        
        with patch('adsboost.tasks.app') as mock_app:
            # Mock the session scope and query results
            mock_session = MagicMock()
            mock_record = MagicMock()
            mock_record.bibcode = "2022ApJ...931...44P"
            mock_record.scix_id = "scix:75M6-3WST-4DM1"
            mock_record.doctype_boost = 1.0
            mock_record.refereed_boost = 1.0
            mock_record.recency_boost = 0.8
            mock_record.boost_factor = 0.933
            mock_record.astrophysics_weight = 1.0
            mock_record.physics_weight = 0.64
            mock_record.earthscience_weight = 0.1
            mock_record.planetary_weight = 0.46
            mock_record.heliophysics_weight = 0.28
            mock_record.general_weight = 0.64
            mock_record.astrophysics_final_boost = 0.933
            mock_record.physics_final_boost = 0.597
            mock_record.earthscience_final_boost = 0.093
            mock_record.planetary_final_boost = 0.429
            mock_record.heliophysics_final_boost = 0.261
            mock_record.general_final_boost = 0.597
            mock_record.created = None
            
            mock_session.query.return_value.all.return_value = [mock_record]
            mock_app.session_scope.return_value.__enter__.return_value = mock_session
            mock_app.session_scope.return_value.__exit__.return_value = None
            mock_app.prepare_output_file.return_value = None
            mock_app.add_record_to_output_file.return_value = None
            
            result = task_export_boost_factors(output_path)
            
            assert result['status'] == 'success'
            assert result['output_path'] == output_path
            mock_app.prepare_output_file.assert_called_once_with(output_path)
            mock_app.add_record_to_output_file.assert_called_once()
    
    def test_task_export_boost_factors_with_specific_bibcodes(self):
        """Test export with specific bibcodes"""
        output_path = "/tmp/test_export.csv"
        bibcodes = ["2022ApJ...931...44P"]
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.query_boost_factors.return_value = [{"bibcode": "2022ApJ...931...44P"}]
            mock_app.prepare_output_file.return_value = None
            mock_app.add_record_to_output_file.return_value = None
            
            result = task_export_boost_factors(output_path, bibcodes=bibcodes)
            
            assert result['status'] == 'success'
            mock_app.query_boost_factors.assert_called_once_with(bibcode="2022ApJ...931...44P")
    
    def test_task_export_boost_factors_with_specific_scix_ids(self):
        """Test export with specific scix_ids"""
        output_path = "/tmp/test_export.csv"
        scix_ids = ["scix:75M6-3WST-4DM1"]
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.query_boost_factors.return_value = [{"scix_id": "scix:75M6-3WST-4DM1"}]
            mock_app.prepare_output_file.return_value = None
            mock_app.add_record_to_output_file.return_value = None
            
            result = task_export_boost_factors(output_path, scix_ids=scix_ids)
            
            assert result['status'] == 'success'
            mock_app.query_boost_factors.assert_called_once_with(scix_id="scix:75M6-3WST-4DM1")
    
    def test_task_export_boost_factors_error(self):
        """Test error handling in export"""
        output_path = "/tmp/test_export.csv"
        
        with patch('adsboost.tasks.app') as mock_app:
            mock_app.prepare_output_file.side_effect = Exception("Export error")
            
            with pytest.raises(Exception, match="Export error"):
                task_export_boost_factors(output_path)
    
    def test_task_integration_workflow(self, sample_record, sample_boost_factors):
        """Test integration workflow: compute -> store -> send to master pipeline"""
        bibcode = sample_record['bibcode']
        scix_id = sample_record['scix_id']
        
        with patch('adsboost.tasks.app') as mock_app:
            # Mock all app methods
            mock_app.compute_final_boost.return_value = sample_boost_factors
            mock_app.store_boost_factors.return_value = None
            mock_app.send_to_master_pipeline.return_value = None
            mock_app.query_boost_factors.return_value = [{
                'bibcode': bibcode,
                'scix_id': scix_id,
                'boost_factor': sample_boost_factors['boost_factor']
            }]
            
            # Test the complete workflow
            computed_factors = task_compute_boost_factors(sample_record)
            assert computed_factors == sample_boost_factors
            
            store_result = task_store_boost_factors(bibcode, scix_id, computed_factors)
            assert store_result == "success"
            
            send_result = task_send_to_master_pipeline(sample_record, computed_factors)
            assert send_result == "success"
            
            query_result = task_query_boost_factors(bibcode=bibcode)
            assert len(query_result) == 1
            assert query_result[0]['bibcode'] == bibcode
            assert query_result[0]['boost_factor'] == sample_boost_factors['boost_factor']

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "-s"])
