#!/usr/bin/env python3
"""
Script to generate expected output files from input files in the stubdata directory.
This allows quick regeneration of expected outputs as input files are modified.
"""

import os
import json
import sys
from pathlib import Path

# Add the parent directory to the path so we can import ADSBoost
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from adsboost.app import ADSBoostCelery


def generate_output_for_input(input_file_path, output_dir):
    """Generate expected output for a single input file."""
    print(f"Processing {input_file_path.name}...")
    
    # Read input file
    with open(input_file_path, 'r') as f:
        input_data = json.load(f)
    
    # Initialize the app
    app = ADSBoostCelery('ADSBoostPipeline')
    
    try:
        # Compute boost factors
        result = app.compute_final_boost(input_data)
        
        # Extract test record info for reference
        test_record_info = {
            'bibcode': input_data.get('bibcode', 'N/A'),
            'scix_id': input_data.get('scix_id', 'N/A'),
            'doctype': input_data.get('bib_data', {}).get('doctype', 'N/A'),
            'classification': []
        }
        
        # Create output structure
        output_data = {
            'test_record_info': test_record_info,
            'doctype_boost': result.get('doctype_boost', 0.0),
            'refereed_boost': result.get('refereed_boost', 0.0),
            'recency_boost': result.get('recency_boost', 0.0),
            'boost_factor': result.get('boost_factor', 0.0),
            'astronomy_weight': result.get('astronomy_weight', 0.0),
            'physics_weight': result.get('physics_weight', 0.0),
            'earth_science_weight': result.get('earth_science_weight', 0.0),
            'planetary_science_weight': result.get('planetary_science_weight', 0.0),
            'heliophysics_weight': result.get('heliophysics_weight', 0.0),
            'general_weight': result.get('general_weight', 0.0),
            'astronomy_final_boost': result.get('astronomy_final_boost', 0.0),
            'physics_final_boost': result.get('physics_final_boost', 0.0),
            'earth_science_final_boost': result.get('earth_science_final_boost', 0.0),
            'planetary_science_final_boost': result.get('planetary_science_final_boost', 0.0),
            'heliophysics_final_boost': result.get('heliophysics_final_boost', 0.0),
            'general_final_boost': result.get('general_final_boost', 0.0)
        }
        
        # Generate output filename
        output_filename = input_file_path.stem + '_expected_output.json'
        output_path = output_dir / output_filename
        
        # Write output file
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"  Generated: {output_path}")
        return True
        
    except Exception as e:
        print(f"  Error processing {input_file_path.name}: {e}")
        return False


def main():
    """Main function to generate outputs for all input files."""
    # Get paths
    stubdata_dir = Path(__file__).parent
    inputs_dir = stubdata_dir / 'inputs'
    outputs_dir = stubdata_dir / 'outputs'
    
    # Ensure outputs directory exists
    outputs_dir.mkdir(exist_ok=True)
    
    # Find all input files
    input_files = list(inputs_dir.glob('*.json'))
    
    if not input_files:
        print("No input files found in stubdata/inputs/")
        return
    
    print(f"Found {len(input_files)} input files")
    print(f"Output directory: {outputs_dir}")
    print()
    
    # Process each input file
    success_count = 0
    for input_file in input_files:
        if generate_output_for_input(input_file, outputs_dir):
            success_count += 1
        print()
    
    print(f"Successfully generated {success_count}/{len(input_files)} output files")
    
    if success_count < len(input_files):
        print("Some files failed to process. Check the error messages above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
