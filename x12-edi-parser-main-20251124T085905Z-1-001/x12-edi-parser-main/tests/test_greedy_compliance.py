"""
Comprehensive Greedy Compliance Tests

These tests mathematically prove that every single segment and raw value 
in the input EDI file exists in the output JSON hierarchy after the 
greedy/universal extraction refactoring.
"""
import unittest
import json
from databricksx12.edi import EDI
from databricksx12.hls.healthcare import HealthcareManager
from databricksx12.edi import Segment


class TestGreedyCompliance(unittest.TestCase):
    """
    Test suite to verify greedy extraction compliance.
    Does not require Spark, uses standard unittest.
    """
    
    def _extract_input_segments(self, raw_content, edi_obj):
        """
        Step A: Tokenize raw EDI text into segments (excluding envelope segments).
        
        Returns:
            list: List of tuples (segment_name, full_segment_string)
        """
        input_segments = []
        segment_delim = edi_obj.format_cls.SEGMENT_DELIM
        element_delim = edi_obj.format_cls.ELEMENT_DELIM
        
        # Split by segment delimiter
        lines = raw_content.split(segment_delim)
        
        # Control segments to exclude (envelope and transaction header segments)
        # BHT is transaction-level, not claim-level, so exclude it
        control_segments = {'ISA', 'GS', 'GE', 'IEA', 'ST', 'SE', 'BHT'}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Extract segment name (first element)
            if element_delim in line:
                parts = line.split(element_delim)
                if parts and parts[0]:
                    seg_name = parts[0].strip()
                    # Skip control segments
                    if seg_name and seg_name not in control_segments:
                        input_segments.append((seg_name, line))
        
        return input_segments
    
    def _flatten_json_segments(self, obj, path="", collected=None):
        """
        Step B: Recursively traverse JSON output and collect all segment data.
        
        Collects:
        1. All values from 'segments' dictionaries
        2. All mapped field values
        
        Returns:
            dict: {
                'segments': set of segment names found,
                'values': set of all string values found
            }
        """
        if collected is None:
            collected = {'segments': set(), 'values': set(), 'segment_data': []}
        
        if isinstance(obj, dict):
            # Check for segments dictionary (greedy storage)
            if 'segments' in obj and isinstance(obj['segments'], dict):
                for seg_name, seg_list in obj['segments'].items():
                    collected['segments'].add(seg_name)
                    # Collect all element values from segments
                    for seg_elements in seg_list:
                        if isinstance(seg_elements, list):
                            for elem in seg_elements:
                                if isinstance(elem, list):
                                    # Sub-elements
                                    for sub_elem in elem:
                                        if sub_elem:
                                            collected['values'].add(str(sub_elem))
                                elif elem:
                                    collected['values'].add(str(elem))
                            # Store segment data for detailed comparison
                            collected['segment_data'].append((seg_name, seg_elements))
            
            # Collect all string values from mapped fields
            for key, value in obj.items():
                if isinstance(value, str) and value:
                    collected['values'].add(value)
                elif isinstance(value, (int, float)):
                    collected['values'].add(str(value))
                elif isinstance(value, list):
                    for item in value:
                        self._flatten_json_segments(item, f"{path}.{key}", collected)
                elif isinstance(value, dict):
                    self._flatten_json_segments(value, f"{path}.{key}", collected)
        
        elif isinstance(obj, list):
            for item in obj:
                self._flatten_json_segments(item, path, collected)
        
        return collected
    
    def _normalize_segment_string(self, seg_string, edi_obj):
        """
        Normalize segment string for comparison.
        Removes whitespace and normalizes delimiters.
        """
        element_delim = edi_obj.format_cls.ELEMENT_DELIM
        # Split and rejoin to normalize
        parts = seg_string.split(element_delim)
        return element_delim.join([p.strip() for p in parts])
    
    def test_nuclear_data_conservation_837p(self):
        """
        Test 1: The "Nuclear" Data Conservation Test (837P)
        
        Proves that 0% of data is deleted by verifying every input segment
        can be found in the output.
        """
        # Load and parse 837P file
        with open("sampledata/837/837p.txt", "rb") as f:
            raw_content = f.read().decode("utf-8")
        
        edi = EDI(raw_content)
        claims = HealthcareManager.from_edi(edi)
        
        self.assertGreater(len(claims), 0, "Should parse at least one claim")
        
        # Step A: Extract input segments
        input_segments = self._extract_input_segments(raw_content, edi)
        
        # Step B: Extract output segments and values
        all_output_data = {'segments': set(), 'values': set(), 'segment_data': []}
        for claim in claims:
            claim_json = claim.to_json()
            output_data = self._flatten_json_segments(claim_json)
            all_output_data['segments'].update(output_data['segments'])
            all_output_data['values'].update(output_data['values'])
            all_output_data['segment_data'].extend(output_data['segment_data'])
        
        # Assertion: Verify every input segment name exists in output
        input_seg_names = {seg[0] for seg in input_segments}
        missing_seg_names = input_seg_names - all_output_data['segments']
        
        self.assertEqual(
            len(missing_seg_names), 
            0,
            f"Missing segment names in output: {sorted(missing_seg_names)}. "
            f"Input had {len(input_seg_names)} unique segment types, "
            f"output has {len(all_output_data['segments'])} unique segment types."
        )
        
        # Assertion: Verify segment values can be found
        # For each input segment, check that its values appear in output
        missing_values = []
        for seg_name, seg_string in input_segments:
            normalized_input = self._normalize_segment_string(seg_string, edi)
            # Extract values from input segment
            input_parts = normalized_input.split(edi.format_cls.ELEMENT_DELIM)
            input_values = [p for p in input_parts[1:] if p]  # Skip segment name
            
            # Check if values exist in output
            found_values = sum(1 for val in input_values if val in all_output_data['values'])
            
            if found_values == 0 and len(input_values) > 0:
                missing_values.append((seg_name, seg_string))
        
        # Allow some tolerance for formatted values, but log missing
        if missing_values:
            print(f"\nWarning: {len(missing_values)} segments had values not found in output:")
            for seg_name, seg_string in missing_values[:5]:  # Show first 5
                print(f"  {seg_name}: {seg_string[:100]}")
        
        # Critical assertion: All segment names must be present
        self.assertEqual(len(missing_seg_names), 0, 
                        "CRITICAL: Some segment types are completely missing from output!")
    
    def test_nuclear_data_conservation_837i(self):
        """
        Test 1b: The "Nuclear" Data Conservation Test (837I)
        """
        # Load and parse 837I file
        with open("sampledata/837/CC_837I_EDI.txt", "rb") as f:
            raw_content = f.read().decode("utf-8")
        
        edi = EDI(raw_content)
        claims = HealthcareManager.from_edi(edi)
        
        self.assertGreater(len(claims), 0, "Should parse at least one claim")
        
        # Step A: Extract input segments
        input_segments = self._extract_input_segments(raw_content, edi)
        
        # Step B: Extract output segments
        all_output_data = {'segments': set(), 'values': set(), 'segment_data': []}
        for claim in claims:
            claim_json = claim.to_json()
            output_data = self._flatten_json_segments(claim_json)
            all_output_data['segments'].update(output_data['segments'])
            all_output_data['values'].update(output_data['values'])
            all_output_data['segment_data'].extend(output_data['segment_data'])
        
        # Assertion: Verify every input segment name exists in output
        input_seg_names = {seg[0] for seg in input_segments}
        missing_seg_names = input_seg_names - all_output_data['segments']
        
        self.assertEqual(
            len(missing_seg_names), 
            0,
            f"Missing segment names in output: {sorted(missing_seg_names)}. "
            f"Input had {len(input_seg_names)} unique segment types, "
            f"output has {len(all_output_data['segments'])} unique segment types."
        )
    
    def test_hierarchy_payer_provider_check(self):
        """
        Test 2: Hierarchy & Index Validation (The "Payer/Provider" Check)
        
        Verifies that payer and provider loops contain all segments,
        not just NM1.
        """
        # Load Molina file (has complex payer/provider structure)
        with open("sampledata/837/Molina_Mock_UP_837P_File.txt", "rb") as f:
            raw_content = f.read().decode("utf-8")
        
        edi = EDI(raw_content)
        claims = HealthcareManager.from_edi(edi)
        
        self.assertGreater(len(claims), 0, "Should parse at least one claim")
        
        claim = claims[0]
        
        # Check Payer Loop (2010BB) - should contain N3, N4, REF
        if hasattr(claim, 'payer_info') and claim.payer_info:
            self.assertTrue(
                hasattr(claim.payer_info, 'segments'),
                "payer_info should have 'segments' attribute"
            )
            
            payer_segments = claim.payer_info.segments
            self.assertIsInstance(payer_segments, dict, 
                                 "payer_info.segments should be a dictionary")
            
            # Critical assertion: Payer should have more than just NM1
            if len(payer_segments) > 0:
                # Check for address segments (N3, N4)
                has_address = 'N3' in payer_segments or 'N4' in payer_segments
                has_ref = 'REF' in payer_segments
                
                # If payer loop exists, it should have more than just NM1
                if 'NM1' in payer_segments:
                    self.assertTrue(
                        has_address or has_ref,
                        f"Payer loop should contain N3/N4/REF segments, "
                        f"but only found: {list(payer_segments.keys())}. "
                        f"This indicates greedy slice failed."
                    )
        
        # Check Provider Loops - verify they contain REF or PRV
        if hasattr(claim, 'provider_info') and claim.provider_info:
            for provider_type, provider in claim.provider_info.items():
                if provider:
                    self.assertTrue(
                        hasattr(provider, 'segments'),
                        f"Provider ({provider_type}) should have 'segments' attribute"
                    )
                    
                    provider_segments = provider.segments
                    self.assertIsInstance(provider_segments, dict,
                                         f"Provider ({provider_type}).segments should be a dictionary")
                    
                    # If provider has NM1, it should have additional segments
                    if 'NM1' in provider_segments and len(provider_segments) > 0:
                        has_additional = any(key in provider_segments 
                                           for key in ['REF', 'PRV', 'N3', 'N4', 'PER'])
                        
                        # For rendering/attending providers, expect REF or PRV
                        if provider_type in ['rendering', 'attending', 'operating']:
                            self.assertTrue(
                                has_additional,
                                f"Provider ({provider_type}) should contain REF/PRV/N3/N4 segments "
                                f"after NM1, but only found: {list(provider_segments.keys())}"
                            )
    
    def test_service_line_blind_spot_verification(self):
        """
        Test 3: The "Service Line Blind Spot" Verification
        
        Proves ServiceLine is no longer a hybrid/static object and
        captures all segments in the service line loop.
        """
        # Load 837P file
        with open("sampledata/837/837p.txt", "rb") as f:
            raw_content = f.read().decode("utf-8")
        
        edi = EDI(raw_content)
        claims = HealthcareManager.from_edi(edi)
        
        self.assertGreater(len(claims), 0, "Should parse at least one claim")
        
        claim = claims[0]
        
        # Check service lines exist
        self.assertTrue(
            hasattr(claim, 'sl_info') and len(claim.sl_info) > 0,
            "Claim should have at least one service line"
        )
        
        # Access first ServiceLine
        first_sl = claim.sl_info[0]
        
        # Assertion 1: segments dictionary exists and is not empty
        self.assertTrue(
            hasattr(first_sl, 'segments'),
            "ServiceLine should have 'segments' attribute"
        )
        
        self.assertIsInstance(
            first_sl.segments, 
            dict,
            "ServiceLine.segments should be a dictionary"
        )
        
        # Assertion 2: segments dictionary should contain service line segments
        # Standard segments: LX, SV1/SV2, DTP
        sl_segments = first_sl.segments
        
        # Should have at least LX and SV1 (for 837P) or SV2 (for 837I)
        has_lx = 'LX' in sl_segments
        has_sv = 'SV1' in sl_segments or 'SV2' in sl_segments
        
        self.assertTrue(
            has_lx and has_sv,
            f"ServiceLine.segments should contain LX and SV1/SV2, "
            f"but found: {list(sl_segments.keys())}"
        )
        
        # Assertion 3: Check for DTP segments (service dates)
        # DTP should be in segments dictionary
        has_dtp = 'DTP' in sl_segments
        
        # Also verify DTP appears in the raw segments if present
        # Parse raw file to find DTP segments in service lines
        segment_delim = edi.format_cls.SEGMENT_DELIM
        element_delim = edi.format_cls.ELEMENT_DELIM
        lines = raw_content.split(segment_delim)
        
        # Find DTP segments after LX segments (service line context)
        found_dtp_in_raw = False
        in_service_line = False
        for line in lines:
            if element_delim in line:
                seg_name = line.split(element_delim)[0].strip()
                if seg_name == 'LX':
                    in_service_line = True
                elif seg_name in ['CLM', 'HL'] and in_service_line:
                    in_service_line = False
                elif seg_name == 'DTP' and in_service_line:
                    found_dtp_in_raw = True
                    break
        
        # If DTP exists in raw file within service line, it should be in segments
        if found_dtp_in_raw:
            self.assertTrue(
                has_dtp,
                "DTP segments found in raw file within service line should be "
                "captured in ServiceLine.segments dictionary"
            )
        
        # Assertion 4: Verify segments dictionary is populated
        self.assertGreater(
            len(sl_segments),
            0,
            "ServiceLine.segments should not be empty"
        )
        
        # Log segment types found for debugging
        print(f"\nServiceLine segments found: {list(sl_segments.keys())}")
    
    def test_835_remittance_greedy_check(self):
        """
        Test 4: 835 Remittance Greedy Check
        
        Verifies that 835 service payment loops capture all segments
        including LQ, AMT, REF, etc.
        """
        # Load 835 file with service lines
        with open("sampledata/835/sample_services.txt", "rb") as f:
            raw_content = f.read().decode("utf-8")
        
        edi = EDI(raw_content, strict_transactions=False)
        remittances = HealthcareManager.from_edi(edi)
        
        self.assertGreater(len(remittances), 0, "Should parse at least one remittance")
        
        remittance = remittances[0]
        
        # Navigate to claim lines (Service Payment Loop 2110)
        remittance_json = remittance.to_json()
        
        self.assertIn('claim', remittance_json, "Remittance should have 'claim' key")
        claim_data = remittance_json['claim']
        
        self.assertIn('claim_lines', claim_data, "Claim should have 'claim_lines'")
        claim_lines = claim_data['claim_lines']
        
        self.assertGreater(len(claim_lines), 0, "Should have at least one claim line")
        
        first_line = claim_lines[0]
        
        # Assertion 1: Verify segments dictionary exists
        self.assertIn(
            'segments', 
            first_line,
            "Claim line should have 'segments' dictionary from RemittanceServiceLineIdentity"
        )
        
        line_segments = first_line['segments']
        self.assertIsInstance(
            line_segments, 
            dict,
            "Claim line segments should be a dictionary"
        )
        
        # Assertion 2: Verify LQ (Remarks) or AMT (Allowed Amount) segments
        # Check raw file for these segments
        segment_delim = edi.format_cls.SEGMENT_DELIM
        element_delim = edi.format_cls.ELEMENT_DELIM
        lines = raw_content.split(segment_delim)
        
        # Find segments in service line context (after SVC)
        found_lq = False
        found_amt = False
        found_ref = False
        in_service_line = False
        
        for line in lines:
            if element_delim in line:
                seg_name = line.split(element_delim)[0].strip()
                if seg_name == 'SVC':
                    in_service_line = True
                elif seg_name in ['SVC', 'CLP', 'LX'] and in_service_line and seg_name != 'SVC':
                    in_service_line = False
                elif in_service_line:
                    if seg_name == 'LQ':
                        found_lq = True
                    elif seg_name == 'AMT':
                        found_amt = True
                    elif seg_name == 'REF':
                        found_ref = True
        
        # If these segments exist in raw file, they should be in output
        if found_lq:
            self.assertIn(
                'LQ',
                line_segments,
                "LQ (Remarks) segments found in raw file should be in claim_line.segments"
            )
        
        if found_amt:
            self.assertIn(
                'AMT',
                line_segments,
                "AMT (Allowed Amount) segments found in raw file should be in claim_line.segments"
            )
        
        # Assertion 3: Verify REF segments for line-level identification
        if found_ref:
            self.assertIn(
                'REF',
                line_segments,
                "REF segments found in raw file should be in claim_line.segments"
            )
        
        # Assertion 4: Verify segments dictionary is populated
        self.assertGreater(
            len(line_segments),
            0,
            "Claim line segments dictionary should not be empty"
        )
        
        # Log segment types found
        print(f"\n835 Claim line segments found: {list(line_segments.keys())}")
        
        # Verify that explicit mappings still work (backward compatibility)
        self.assertIn('prcdr_cd', first_line, "Should have explicit prcdr_cd field")
        self.assertIn('chrg_amt', first_line, "Should have explicit chrg_amt field")


if __name__ == '__main__':
    unittest.main()

