#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import json
import os
from datetime import datetime
from adsboost.app import ADSBoostCelery

class TestAppFunctions:
    """Test all functions in app.py using static input/output files"""
    
    @pytest.fixture
    def app(self):
        """Create app instance - it will read its own config file"""
        app = ADSBoostCelery('ADSBoostPipeline')
        return app
    
    def get_test_files(self):
        """Get all test input/output file pairs"""
        base_dir = os.path.dirname(__file__)
        inputs_dir = os.path.join(base_dir, 'stubdata', 'inputs')
        outputs_dir = os.path.join(base_dir, 'stubdata', 'outputs')
        
        test_files = []
        for input_file in os.listdir(inputs_dir):
            if input_file.endswith('.json'):
                # Extract the base name (e.g., 'test_astronomy_record' from 'test_astronomy_record.json')
                base_name = input_file[:-5]  # Remove .json extension
                output_file = f"{base_name}_expected_output.json"
                output_path = os.path.join(outputs_dir, output_file)
                
                if os.path.exists(output_path):
                    test_files.append({
                        'name': base_name,
                        'input': os.path.join(inputs_dir, input_file),
                        'output': output_path
                    })
        
        return test_files
    
    def test_compute_boost_factors_basic(self, app):
        """Test basic boost factor computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run basic function tests")
        
        for test_case in test_files:
            print(f"\nTesting compute_final_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            boost_factors = app.compute_final_boost(test_record)
            
            # Check that all required fields are present
            required_fields = [
                'refereed_boost', 'doctype_boost', 'recency_boost', 'boost_factor',
                'astronomy_weight', 'physics_weight', 'earth_science_weight',
                'planetary_science_weight', 'heliophysics_weight', 'general_weight',
                'astronomy_final_boost', 'physics_final_boost', 'earth_science_final_boost',
                'planetary_science_final_boost', 'heliophysics_final_boost', 'general_final_boost'
            ]
            
            for field in required_fields:
                assert field in boost_factors, f"Missing field: {field} in {test_case['name']}"
                assert isinstance(boost_factors[field], (int, float)), f"Field {field} should be numeric in {test_case['name']}"
                assert boost_factors[field] >= 0, f"Field {field} should be non-negative in {test_case['name']}"
            
            print(f"  ✅ {test_case['name']} - All required fields present and valid")
        
        print(f"\nAll {len(test_files)} test records passed basic boost factor computation!")
    
    def test_compute_refereed_boost(self, app):
        """Test refereed boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run refereed boost tests")
        
        for test_case in test_files:
            print(f"\nTesting compute_refereed_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            boost = app.compute_refereed_boost(test_record)
            assert boost in [0.0, 1.0], f"Refereed boost should be 0.0 or 1.0, got {boost} in {test_case['name']}"
            print(f"  ✅ {test_case['name']} - Refereed boost: {boost}")
        
        print(f"\nAll {len(test_files)} test records passed refereed boost computation!")
        
        # Test edge cases with minimal records
        print("\nTesting edge cases with minimal records:")
        
        # Test refereed record
        refereed_record = {
            "metrics": {"refereed": True},
            "bib_data": {"refereed": False}  # metrics should take precedence
        }
        boost = app.compute_refereed_boost(refereed_record)
        assert boost == 1.0
        print("  ✅ Minimal refereed record (metrics precedence)")
        
        # Test non-refereed record
        non_refereed_record = {
            "metrics": {"refereed": False},
            "bib_data": {"refereed": False}
        }
        boost = app.compute_refereed_boost(non_refereed_record)
        assert boost == 0.0
        print("  ✅ Minimal non-refereed record")
        
        # Test record with only bib_data
        bib_data_record = {
            "bib_data": {"refereed": True}
        }
        boost = app.compute_refereed_boost(bib_data_record)
        assert boost == 1.0
        print("  ✅ Record with only bib_data")
    
    def test_compute_doctype_boost(self, app):
        """Test document type boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run doctype boost tests")
        
        for test_case in test_files:
            print(f"\nTesting compute_doctype_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            boost = app.compute_doctype_boost(test_record)
            assert isinstance(boost, (int, float)), f"Doctype boost should be numeric in {test_case['name']}"
            assert boost >= 0, f"Doctype boost should be non-negative in {test_case['name']}"
            print(f"  ✅ {test_case['name']} - Doctype boost: {boost}")
        
        print(f"\nAll {len(test_files)} test records passed doctype boost computation!")
        
        # Test edge case with minimal record
        print("\nTesting edge case with minimal record:")
        minimal_record = {
            "bib_data": {"doctype": "article"}
        }
        boost = app.compute_doctype_boost(minimal_record)
        assert isinstance(boost, (int, float))
        assert boost >= 0
        print("  ✅ Minimal record with doctype")
    
    def test_compute_recency_boost(self, app):
        """Test recency boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run recency boost tests")
        
        for test_case in test_files:
            print(f"\nTesting compute_recency_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            boost = app.compute_recency_boost(test_record)
            assert isinstance(boost, (int, float)), f"Recency boost should be numeric in {test_case['name']}"
            assert boost > 0, f"Recency boost should be positive in {test_case['name']}"
            print(f"  ✅ {test_case['name']} - Recency boost: {boost}")
        
        print(f"\nAll {len(test_files)} test records passed recency boost computation!")
        
        # Test edge cases with specific dates
        print("\nTesting edge cases with specific dates:")
        
        # Test recent record
        recent_record = {
            "bib_data": {
                "pubdate": "2024-01-01",
                "entry_date": "2024-01-15"
            }
        }
        boost = app.compute_recency_boost(recent_record)
        assert isinstance(boost, (int, float))
        assert boost > 0
        print("  ✅ Recent record (2024)")
        
        # Test old record (should return 1.0 after 24 months)
        old_record = {
            "bib_data": {
                "pubdate": "2020-01-01",
                "entry_date": "2020-01-15"
            }
        }
        boost = app.compute_recency_boost(old_record)
        assert boost == 1.0
        print("  ✅ Old record (2020) - should return 1.0")
    
    def test_compute_collection_weights(self, app):
        """Test collection weight computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run collection weight tests")
        
        for test_case in test_files:
            print(f"\nTesting compute_collection_weights with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            weights = app.compute_collection_weights(test_record)
            
            required_weight_fields = [
                'astronomy_weight', 'physics_weight', 'earth_science_weight',
                'planetary_science_weight', 'heliophysics_weight', 'general_weight'
            ]
            
            for field in required_weight_fields:
                assert field in weights, f"Missing weight field: {field} in {test_case['name']}"
                assert isinstance(weights[field], (int, float)), f"Weight {field} should be numeric in {test_case['name']}"
                assert 0 <= weights[field] <= 1, f"Weight {field} should be between 0 and 1 in {test_case['name']}"
            
            print(f"  ✅ {test_case['name']} - All collection weights valid")
        
        print(f"\nAll {len(test_files)} test records passed collection weight computation!")
        
        # Test edge case with minimal record
        print("\nTesting edge case with minimal record:")
        astronomy_record = {
            "classifications": {"database": ["astronomy"]}
        }
        weights = app.compute_collection_weights(astronomy_record)
        
        for field in required_weight_fields:
            assert field in weights, f"Missing weight field: {field}"
            assert isinstance(weights[field], (int, float)), f"Weight {field} should be numeric"
            assert 0 <= weights[field] <= 1, f"Weight {field} should be between 0 and 1"
        
        # Astronomy should have highest weight for astronomy record
        assert weights['astronomy_weight'] >= weights['physics_weight']
        print("  ✅ Minimal astronomy record - weights computed correctly")
    
    def test_compute_final_boost(self, app):
        """Test final boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run final boost tests")
        
        for test_case in test_files:
            print(f"\nTesting compute_final_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            final_boosts = app.compute_final_boost(test_record)
            
            required_final_fields = [
                'astronomy_final_boost', 'physics_final_boost', 'earth_science_final_boost',
                'planetary_science_final_boost', 'heliophysics_final_boost', 'general_final_boost'
            ]
            
            for field in required_final_fields:
                assert field in final_boosts, f"Missing final boost field: {field} in {test_case['name']}"
                assert isinstance(final_boosts[field], (int, float)), f"Final boost {field} should be numeric in {test_case['name']}"
                assert final_boosts[field] >= 0, f"Final boost {field} should be non-negative in {test_case['name']}"
            
            print(f"  ✅ {test_case['name']} - All final boosts computed correctly")
        
        print(f"\nAll {len(test_files)} test records passed final boost computation!")
        
        # Test edge case with minimal data
        print("\nTesting edge case with minimal data:")
        minimal_boost_factors = {
            'refereed_boost': 1.0,
            'doctype_boost': 0.8,
            'recency_boost': 0.9
        }
        
        minimal_record = {
            "classifications": {"database": ["astronomy"]}
        }
        
        final_boosts = app.compute_final_boost(minimal_record)
        
        for field in required_final_fields:
            assert field in final_boosts, f"Missing final boost field: {field}"
            assert isinstance(final_boosts[field], (int, float)), f"Final boost {field} should be numeric"
            assert final_boosts[field] >= 0, f"Final boost {field} should be non-negative"
        
        print("  ✅ Minimal data - final boosts computed correctly")
    
    def test_boost_pipeline_outputs(self, app):
        """Test that the boost pipeline produces expected outputs for all test records"""
        test_files = self.get_test_files()
        
        if len(test_files) == 0:
            pytest.skip("No test files found - skipping file-based tests")
        
        for test_case in test_files:
            print(f"\nTesting: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Load expected output
            with open(test_case['output'], 'r') as f:
                expected_output = json.load(f)
            
            # Run the boost pipeline
            actual_output = app.compute_final_boost(test_record)
            
            # Test record info for reference
            print(f"  Test Record: {expected_output['test_record_info']['bibcode']}")
            print(f"     Doctype: {expected_output['test_record_info']['doctype']}")
            # Display collections from classifications.database (primary) or bib_data.database (fallback)
            collections = []
            if 'classifications' in test_record and 'database' in test_record['classifications']:
                collections = test_record['classifications']['database']
            elif 'bib_data' in test_record and 'database' in test_record['bib_data']:
                collections = test_record['bib_data']['database']
            
            print(f"     Collections: {collections if collections else 'None'}")
            
            # Compare basic boost factors
            print("  Basic Boost Factors:")
            for field in ['doctype_boost', 'refereed_boost', 'recency_boost', 'boost_factor']:
                expected = expected_output[field]
                actual = actual_output[field]
                assert abs(actual - expected) < 0.001, f"{field}: expected {expected}, got {actual}"
                print(f"     {field}: {actual} ✓")
            
            # Compare collection weights
            print("  Collection Weights:")
            for field in ['astronomy_weight', 'physics_weight', 'earth_science_weight', 
                         'planetary_science_weight', 'heliophysics_weight', 'general_weight']:
                expected = expected_output[field]
                actual = actual_output[field]
                assert abs(actual - expected) < 0.001, f"{field}: expected {expected}, got {actual}"
                print(f"     {field}: {actual} ✓")
            
            # Compare final discipline boosts
            print("  Final Discipline Boosts:")
            for field in ['astronomy_final_boost', 'physics_final_boost', 'earth_science_final_boost',
                         'planetary_science_final_boost', 'heliophysics_final_boost', 'general_final_boost']:
                expected = expected_output[field]
                actual = actual_output[field]
                assert abs(actual - expected) < 0.001, f"{field}: expected {expected}, got {actual}"
                print(f"     {field}: {actual} ✓")
            
            print(f"  ✅ {test_case['name']} - All tests passed!")
        
        print(f"\nAll {len(test_files)} test cases passed successfully!")
    
    def test_query_boost_factors(self, app):
        """Test querying boost factors"""
        # This test requires a database connection, so we'll test the method exists
        assert hasattr(app, 'query_boost_factors'), "query_boost_factors method should exist"
        assert callable(app.query_boost_factors), "query_boost_factors should be callable"
    
    def test_store_boost_factors(self, app):
        """Test storing boost factors"""
        # This test requires a database connection, so we'll test the method exists
        assert hasattr(app, 'store_boost_factors'), "store_boost_factors method should exist"
        assert callable(app.store_boost_factors), "store_boost_factors should be callable"
    
    def test_send_to_master_pipeline(self, app):
        """Test sending to master pipeline"""
        # This test requires a database connection, so we'll test the method exists
        assert hasattr(app, 'send_to_master_pipeline'), "send_to_master_pipeline method should exist"
        assert callable(app.send_to_master_pipeline), "send_to_master_pipeline should be callable"

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "-s"])
