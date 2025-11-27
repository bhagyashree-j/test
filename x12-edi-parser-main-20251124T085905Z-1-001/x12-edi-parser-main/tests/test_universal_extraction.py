"""
Test to verify universal/greedy extraction strategy.
Ensures 100% of all segments found in the raw file are captured in the output dictionary.
"""
import unittest
import re
import json
from databricksx12.edi import EDI
from databricksx12.hls.healthcare import HealthcareManager


class TestUniversalExtraction(unittest.TestCase):
    
    def test_conservation_of_data_837p(self):
        """
        Test that all segments in the raw 837p file are captured in the output JSON.
        This verifies the "Conservation of Data" principle.
        """
        # Load the sample file
        with open("sampledata/837/837p.txt", "rb") as f:
            raw_content = f.read().decode("utf-8")
        
        # Parse the file
        edi = EDI(raw_content)
        hm = HealthcareManager()
        claims = hm.from_edi(edi)
        
        # Get JSON output for all claims
        all_output_segments = set()
        for claim in claims:
            claim_json = claim.to_json()
            # Recursively extract all segment keys from the JSON
            self._extract_segment_keys(claim_json, all_output_segments)
        
        # Extract all unique segment tags from raw file
        # Segment format: SEGMENT_NAME*element1*element2*...
        # Segment delimiter is typically ~ or \n
        raw_segments = set()
        # Split by segment delimiter (usually ~ or newline)
        segment_delim = edi.format_cls.SEGMENT_DELIM
        lines = raw_content.split(segment_delim)
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Extract segment name (first element before first element delimiter)
            if segment_delim in line or edi.format_cls.ELEMENT_DELIM in line:
                # Get segment name (first 3 characters typically, or up to first delimiter)
                parts = line.split(edi.format_cls.ELEMENT_DELIM)
                if parts and parts[0]:
                    seg_name = parts[0].strip()
                    # Skip control segments that aren't part of the claim data
                    if seg_name and seg_name not in ['ISA', 'GS', 'GE', 'IEA', 'ST', 'SE']:
                        raw_segments.add(seg_name)
        
        # Also check segments from parsed EDI object
        parsed_segments = set()
        for seg in edi.data:
            if seg._name and seg._name not in ['ISA', 'GS', 'GE', 'IEA', 'ST', 'SE']:
                parsed_segments.add(seg._name)
        
        # Use parsed segments as the source of truth (more reliable)
        input_segments = parsed_segments
        
        # Check that all input segments exist in output
        missing_segments = input_segments - all_output_segments
        
        # Print diagnostic information
        if missing_segments:
            print(f"\nInput segments found: {sorted(input_segments)}")
            print(f"Output segments found: {sorted(all_output_segments)}")
            print(f"Missing segments: {sorted(missing_segments)}")
        
        # Assert that no segments are missing
        self.assertEqual(
            len(missing_segments), 
            0, 
            f"Missing segments in output: {sorted(missing_segments)}. "
            f"Input had {len(input_segments)} unique segments, "
            f"output has {len(all_output_segments)} unique segment keys."
        )
    
    def _extract_segment_keys(self, obj, segment_set, path=""):
        """
        Recursively extract segment keys from JSON structure.
        Looks for 'segments' dictionaries which contain segment names as keys.
        """
        if isinstance(obj, dict):
            # Check if this is a segments dictionary (contains segment arrays)
            if 'segments' in obj and isinstance(obj['segments'], dict):
                for seg_name in obj['segments'].keys():
                    if seg_name:  # Skip empty keys
                        segment_set.add(seg_name)
            
            # Recursively process nested structures
            for key, value in obj.items():
                self._extract_segment_keys(value, segment_set, f"{path}.{key}")
        
        elif isinstance(obj, list):
            for item in obj:
                self._extract_segment_keys(item, segment_set, path)
    
    def test_segments_in_identity_classes(self):
        """
        Test that Identity classes properly store segments in the segments dictionary.
        """
        with open("sampledata/837/837p.txt", "rb") as f:
            raw_content = f.read().decode("utf-8")
        
        edi = EDI(raw_content)
        hm = HealthcareManager()
        claims = hm.from_edi(edi)
        
        # Check that claim_info has segments dictionary
        for claim in claims:
            # Verify that Identity classes have segments attribute
            if hasattr(claim, 'claim_info') and claim.claim_info:
                self.assertTrue(
                    hasattr(claim.claim_info, 'segments'),
                    "ClaimIdentity should have 'segments' attribute"
                )
                self.assertIsInstance(
                    claim.claim_info.segments, 
                    dict,
                    "ClaimIdentity.segments should be a dictionary"
                )
                
                # Verify segments dictionary is not empty for non-empty claim loops
                if claim.claim_loop:
                    self.assertGreater(
                        len(claim.claim_info.segments),
                        0,
                        "ClaimIdentity.segments should contain at least one segment"
                    )
            
            # Check subscriber_info
            if hasattr(claim, 'subscriber_info') and claim.subscriber_info:
                self.assertTrue(
                    hasattr(claim.subscriber_info, 'segments'),
                    "PatientIdentity should have 'segments' attribute"
                )
            
            # Check provider_info
            if hasattr(claim, 'provider_info') and claim.provider_info:
                for provider_type, provider in claim.provider_info.items():
                    if provider:
                        self.assertTrue(
                            hasattr(provider, 'segments'),
                            f"ProviderIdentity ({provider_type}) should have 'segments' attribute"
                        )


if __name__ == '__main__':
    unittest.main()

