#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import pytest
import json
import os
from datetime import datetime
from adsboost.app import ADSBoostCelery
from adsputils import load_config, setup_logging
# ============================= INITIALIZATION ==================================== #
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)

logger = setup_logging('run.py', proj_home=proj_home,
                       level=config.get('LOGGING_LEVEL', 'DEBUG'),
                       attach_stdout=config.get('LOG_STDOUT', False))

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
            logger.info(f"\nTesting compute_final_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            boost_factors = app.compute_final_boost(test_record)
            
            # Check that all required fields are present
            required_fields = (
                ['refereed_boost', 'doctype_boost', 'recency_boost', 'boost_factor']
                + [f'{d}_weight' for d in app.config['COLLECTIONS']]
                + [f'{d}_final_boost' for d in app.config['COLLECTIONS']]
            )
            
            for field in required_fields:
                assert field in boost_factors, f"Missing field: {field} in {test_case['name']}"
                assert isinstance(boost_factors[field], (int, float)), f"Field {field} should be numeric in {test_case['name']}"
                assert boost_factors[field] >= 0, f"Field {field} should be non-negative in {test_case['name']}"
            
            logger.info(f"   {test_case['name']} - All required fields present and valid")
        
        logger.info(f"\nAll {len(test_files)} test records passed basic boost factor computation!")
    
    def test_compute_refereed_boost(self, app):
        """Test refereed boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run refereed boost tests")
        
        for test_case in test_files:
            logger.info(f"\nTesting compute_refereed_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            boost = app.compute_refereed_boost(test_record)
            assert boost in [0.0, 1.0], f"Refereed boost should be 0.0 or 1.0, got {boost} in {test_case['name']}"
            logger.info(f"   {test_case['name']} - Refereed boost: {boost}")
        
        logger.info(f"\nAll {len(test_files)} test records passed refereed boost computation!")
        
        # Test edge cases with minimal records
        logger.info("\nTesting edge cases with minimal records:")
        
        # Test refereed record
        refereed_record = {
            "metrics": {"refereed": True},
            "bib_data": {"refereed": False}  # metrics should take precedence
        }
        boost = app.compute_refereed_boost(refereed_record)
        assert boost == 1.0
        logger.info("   Minimal refereed record (metrics precedence)")
        
        # Test non-refereed record
        non_refereed_record = {
            "metrics": {"refereed": False},
            "bib_data": {"refereed": False}
        }
        boost = app.compute_refereed_boost(non_refereed_record)
        assert boost == 0.0
        logger.info("   Minimal non-refereed record")
        
        # Test record with only bib_data
        bib_data_record = {
            "bib_data": {"refereed": True}
        }
        boost = app.compute_refereed_boost(bib_data_record)
        assert boost == 1.0
        logger.info("   Record with only bib_data")
    
    def test_compute_doctype_boost(self, app):
        """Test document type boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run doctype boost tests")
        
        for test_case in test_files:
            logger.info(f"\nTesting compute_doctype_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            boost = app.compute_doctype_boost(test_record)
            assert isinstance(boost, (int, float)), f"Doctype boost should be numeric in {test_case['name']}"
            assert boost >= 0, f"Doctype boost should be non-negative in {test_case['name']}"
            logger.info(f"   {test_case['name']} - Doctype boost: {boost}")
        
        logger.info(f"\nAll {len(test_files)} test records passed doctype boost computation!")
        
        # Test edge case with minimal record
        logger.info("\nTesting edge case with minimal record:")
        minimal_record = {
            "bib_data": {"doctype": "article"}
        }
        boost = app.compute_doctype_boost(minimal_record)
        assert isinstance(boost, (int, float))
        assert boost >= 0
        logger.info("   Minimal record with doctype")
    
    def test_compute_recency_boost(self, app):
        """Test recency boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run recency boost tests")
        
        for test_case in test_files:
            logger.info(f"\nTesting compute_recency_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            boost = app.compute_recency_boost(test_record)
            assert isinstance(boost, (int, float)), f"Recency boost should be numeric in {test_case['name']}"
            assert boost > 0, f"Recency boost should be positive in {test_case['name']}"
            logger.info(f"   {test_case['name']} - Recency boost: {boost}")
        
        logger.info(f"\nAll {len(test_files)} test records passed recency boost computation!")
        
        # Test edge cases with specific dates
        logger.info("\nTesting edge cases with specific dates:")
        
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
        logger.info("   Recent record (2024)")
        
        # Test old record (should return 1.0 after 24 months)
        old_record = {
            "bib_data": {
                "pubdate": "2020-01-01",
                "entry_date": "2020-01-15"
            }
        }
        boost = app.compute_recency_boost(old_record)
        assert boost == 1.0
        logger.info("   Old record (2020) - should return 1.0")
    
    def test_compute_collection_weights(self, app):
        """Test collection weight computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run collection weight tests")
        
        for test_case in test_files:
            logger.info(f"\nTesting compute_collection_weights with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            weights = app.compute_collection_weights(test_record)
            
            required_weight_fields = [f'{d}_weight' for d in app.config['COLLECTIONS']]
            
            for field in required_weight_fields:
                assert field in weights, f"Missing weight field: {field} in {test_case['name']}"
                assert isinstance(weights[field], (int, float)), f"Weight {field} should be numeric in {test_case['name']}"
                assert 0 <= weights[field] <= 1, f"Weight {field} should be between 0 and 1 in {test_case['name']}"
            
            logger.info(f"   {test_case['name']} - All collection weights valid")
        
        logger.info(f"\nAll {len(test_files)} test records passed collection weight computation!")
        
        # Test edge case with minimal record
        logger.info("\nTesting edge case with minimal record:")
        astronomy_record = {
            "classifications": ["astronomy"]
        }
        weights = app.compute_collection_weights(astronomy_record)
        
        for field in required_weight_fields:
            assert field in weights, f"Missing weight field: {field}"
            assert isinstance(weights[field], (int, float)), f"Weight {field} should be numeric"
            assert 0 <= weights[field] <= 1, f"Weight {field} should be between 0 and 1"
        
        # An astronomy-only record is ranked as 'astronomy' in every discipline
        rankings = app.config['COLLECTION_RANKINGS']
        ranks = sorted({r for d in rankings.values() for r in d.values() if r is not None})
        rank_to_weight = {r: 1.0 - 0.9 * i / (len(ranks) - 1) for i, r in enumerate(ranks)}
        for discipline in app.config['COLLECTIONS']:
            expected = rank_to_weight[rankings[discipline]['astronomy']]
            assert weights[f'{discipline}_weight'] == pytest.approx(expected), \
                f"astronomy-only record in {discipline} search: expected {expected}, got {weights[f'{discipline}_weight']}"

    def test_resolve_collection_hierarchy(self, app):
        """The astronomy umbrella is dropped next to a subcollection and kept on its own"""
        resolve = app._resolve_collection_hierarchy
        assert resolve(['astronomy']) == ['astronomy']
        assert resolve(['astronomy', 'physics']) == ['astronomy', 'physics']
        assert resolve(['astronomy', 'heliophysics']) == ['heliophysics']
        assert resolve(['planetary', 'astronomy']) == ['planetary']
        assert resolve(['astronomy', 'astrophysics', 'heliophysics']) == ['astrophysics', 'heliophysics']
        assert resolve(['physics', 'general']) == ['physics', 'general']
        assert resolve([]) == []

    def test_astronomy_ranked_independently_of_astrophysics(self, app):
        """Astronomy's ranks move astronomy-only records, not astrophysics-labelled ones.

        Uses its own ranking table so the shipped rankings can change freely.
        Ranks 1-6 are in use, so 1=1.00, 2=0.82, 3=0.64, 4=0.46, 5=0.28, 6=0.10.
        """
        cfg = copy.deepcopy(dict(app.config))
        cfg['COLLECTIONS'] = ['astrophysics', 'heliophysics', 'general']
        cfg['COLLECTION_RANKINGS'] = {
            'astrophysics': {'astrophysics': 1, 'physics': 2, 'general': 3, 'astronomy': 4,
                             'planetary': 5, 'heliophysics': 6, 'earthscience': 6},
            'heliophysics': {'heliophysics': 1, 'astronomy': 2, 'physics': 3, 'planetary': 3,
                             'general': 3, 'earthscience': 5, 'astrophysics': 6},
            'general': {c: 1 for c in ('astrophysics', 'astronomy', 'heliophysics', 'planetary',
                                       'physics', 'earthscience', 'general')},
        }
        app.config = cfg

        astronomy_only = app.compute_collection_weights({'bib_data': {'database': ['astronomy']}})
        assert astronomy_only['astrophysics_weight'] == pytest.approx(0.46)
        assert astronomy_only['heliophysics_weight'] == pytest.approx(0.82)
        assert astronomy_only['general_weight'] == pytest.approx(1.0)

        # Astrophysics-labelled records keep the astrophysics ranks, and the
        # umbrella is dropped so astronomy's better heliophysics rank does not
        # leak onto them
        labelled = app.compute_collection_weights({
            'classifications': ['astrophysics'],
            'bib_data': {'database': ['astronomy']}
        })
        assert labelled['astrophysics_weight'] == pytest.approx(1.0)
        assert labelled['heliophysics_weight'] == pytest.approx(0.1)

        # Astronomy plus physics takes the best of both in each discipline.
        # Physics is a collection, not a discipline, so it gets no weight of its own.
        with_physics = app.compute_collection_weights({'bib_data': {'database': ['astronomy', 'physics']}})
        assert with_physics['astrophysics_weight'] == pytest.approx(0.82)  # physics rank 2 beats astronomy rank 4
        assert with_physics['heliophysics_weight'] == pytest.approx(0.82)  # astronomy rank 2 beats physics rank 3
        assert 'physics_weight' not in with_physics

    def test_compute_final_boost(self, app):
        """Test final boost computation using ALL test records"""
        test_files = self.get_test_files()
        if not test_files:
            pytest.skip("No test files found - cannot run final boost tests")
        
        for test_case in test_files:
            logger.info(f"\nTesting compute_final_boost with: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Test with actual test data
            final_boosts = app.compute_final_boost(test_record)
            
            required_final_fields = [f'{d}_final_boost' for d in app.config['COLLECTIONS']]
            
            for field in required_final_fields:
                assert field in final_boosts, f"Missing final boost field: {field} in {test_case['name']}"
                assert isinstance(final_boosts[field], (int, float)), f"Final boost {field} should be numeric in {test_case['name']}"
                assert final_boosts[field] >= 0, f"Final boost {field} should be non-negative in {test_case['name']}"
            
            logger.info(f"   {test_case['name']} - All final boosts computed correctly")
        
        logger.info(f"\nAll {len(test_files)} test records passed final boost computation!")
        
        # Test edge case with minimal data
        logger.info("\nTesting edge case with minimal data:")
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
        
        logger.info("   Minimal data - final boosts computed correctly")
    
    def test_boost_pipeline_outputs(self, app):
        """Test that the boost pipeline produces expected outputs for all test records"""
        test_files = self.get_test_files()
        
        if len(test_files) == 0:
            pytest.skip("No test files found - skipping file-based tests")
        
        for test_case in test_files:
            logger.info(f"\nTesting: {test_case['name']}")
            
            # Load test record
            with open(test_case['input'], 'r') as f:
                test_record = json.load(f)
            
            # Load expected output
            with open(test_case['output'], 'r') as f:
                expected_output = json.load(f)
            
            # Run the boost pipeline
            actual_output = app.compute_final_boost(test_record)
            
            # Test record info for reference
            logger.info(f"  Test Record: {expected_output['test_record_info']['bibcode']}")
            logger.info(f"     Doctype: {expected_output['test_record_info']['doctype']}")
            # Display collections from classifications.database (primary) or bib_data.database (fallback)
            collections = []
            if 'classifications' in test_record and 'database' in test_record['classifications']:
                collections = test_record['classifications']['database']
            elif 'bib_data' in test_record and 'database' in test_record['bib_data']:
                collections = test_record['bib_data']['database']
            
            logger.info(f"     Collections: {collections if collections else 'None'}")
            
            # Compare basic boost factors
            logger.info("  Basic Boost Factors:")
            for field in ['doctype_boost', 'refereed_boost', 'recency_boost', 'boost_factor']:
                expected = expected_output[field]
                actual = actual_output[field]
                assert abs(actual - expected) < 0.001, f"{field}: expected {expected}, got {actual}"
                logger.info(f"     {field}: {actual} ✓")
            
            # Compare collection weights
            logger.info("  Collection Weights:")
            for field in [f'{d}_weight' for d in app.config['COLLECTIONS']]:
                expected = expected_output[field]
                actual = actual_output[field]
                assert abs(actual - expected) < 0.001, f"{field}: expected {expected}, got {actual}"
                logger.info(f"     {field}: {actual} ✓")
            
            # Compare final discipline boosts
            logger.info("  Final Discipline Boosts:")
            for field in [f'{d}_final_boost' for d in app.config['COLLECTIONS']]:
                expected = expected_output[field]
                actual = actual_output[field]
                assert abs(actual - expected) < 0.001, f"{field}: expected {expected}, got {actual}"
                logger.info(f"     {field}: {actual} ✓")
            
            logger.info(f"   {test_case['name']} - All tests passed!")
        
        logger.info(f"\nAll {len(test_files)} test cases passed successfully!")
    
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
