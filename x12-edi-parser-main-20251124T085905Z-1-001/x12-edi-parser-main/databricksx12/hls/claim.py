from databricksx12.edi import EDI, AnsiX12Delim, Segment
from databricksx12.hls.loop import Loop
from databricksx12.hls.identities import *
from typing import List, Dict
from collections import defaultdict


#
# Base claim builder (transaction -> 1 or more claims)
#


class ClaimBuilder(EDI):
    #
    # Given claim type (837i, 837p, etc), segments, and delim class, build claim level classes
    #
    def __init__(self, trnx_type_cls, trnx_data, delim_cls=AnsiX12Delim):
        self.data = trnx_data
        self.format_cls = delim_cls
        self.trnx_cls = trnx_type_cls
        self.loop = Loop(trnx_data)

    #
    # Builds a claim object from
    #
    # @param clm_segment - the claim segment of claim to build
    # @param idx - the index of the claim segment in the data
    #
    #  @return the class containing the relevent claim information
    #
    def build_claim(self, clm_segment, idx):
        return self.trnx_cls(
            sender_receiver_loop=self.get_submitter_receiver_loop(idx),
            billing_loop=self.loop.get_loop_segments(idx, "2000A"),
            subscriber_loop=self.loop.get_loop_segments(idx, "2000B"),
            patient_loop=self.loop.get_loop_segments(idx, "2000C"),
            claim_loop=self.get_claim_loop(idx),
            sl_loop=self.get_service_line_loop(idx),  # service line loop
        )


        
    #
    # https://datainsight.health/edi/payments/dollars-separate/
    #  trx_header_loop = 0000
    #  payer_loop = 1000A
    #  payee_loop = 1000B
    #  header_number_loop = 2000
    #  clm_payment_loop = 2100
    #  srv_payment_loop = 2110
    def build_remittance(self, pay_segment, idx):
        return self.trnx_cls(trx_header_loop = self.data[0:self.index_of_segment(self.data, "N1")]
                             ,payer_loop = self.data[self.index_of_segment(self.data, "N1"):self.index_of_segment(self.data, "N1", self.index_of_segment(self.data, "N1")+1)]
                             ,payee_loop = self.data[self.index_of_segment(self.data, "N1", self.index_of_segment(self.data, "N1")+1): self.index_of_segment(self.data, "LX")]
                             ,clm_loop = self.data[idx:min(
                                 list(filter(lambda x: x > 0, [self.index_of_segment(self.data, "LX", idx+1), 
                                  self.index_of_segment(self.data, "CLP", idx+1),
                                  self.index_of_segment(self.data, "SE", idx+1),
                                  len(self.data)])
                                ))]
                             ,trx_summary_loop = self.data[max(0,
                                self.last_index_of_segment(self.data, "LX"),
                                self.last_index_of_segment(self.data, "CLP"),
                                self.last_index_of_segment(self.data, "SVC")
                             ):]
                             ,header_number_loop = self.data[self.index_of_segment(self.data, "LX"):idx]
                            )

    def build_enrollment(self, pay_segment, idx):
        return self.trnx_cls(
            enrollment_member = self.data[self.index_of_segment(self.data, "INS"): self.index_of_segment(self.data, "SE")],
            health_plan_loop=self.data[self.index_of_segment(self.data, "HD"): self.last_index_of_segment(self.data, "DTP")+1]
        )
    #
    # Determine claim loop: starts at the clm index and ends at LX segment, or CLM segment, or end of data
    #
    def get_claim_loop(self, clm_idx):
        sl_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("LX"))))
        clm_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("CLM"))))

        if sl_start_indexes:
            clm_end_idx = min(sl_start_indexes)
        elif clm_indexes:
            clm_end_idx = min(clm_indexes + [len(self.data)])
        else:
            clm_end_idx = len(self.data)
        
        return self.data[clm_idx:clm_end_idx]

    #
    # fetch the indices of LX and CLM segments that are beyond the current clm index
    #
    def get_service_line_loop(self, clm_idx):
        sl_starts = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("LX"))))
        if not sl_starts:
            return []
        sl_start = min(sl_starts)
        clm_idxs = list(map(lambda x: x[0],filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("CLM"))))
        se_idxs = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("SE"))))
        sl_end = min(clm_idxs + se_idxs + [len(self.data)])
        return self.data[sl_start:sl_end]

    def get_submitter_receiver_loop(self, clm_idx):
        bht_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] < clm_idx, self.segments_by_name_index("BHT"))))
        bht_end_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] < clm_idx and x[1].element(3) == '20', self.segments_by_name_index("HL"))))
        if bht_start_indexes:
            sub_rec_start_idx = max(bht_start_indexes)
            sub_rec_end_idx = max(bht_end_indexes)

            return self.data[sub_rec_start_idx:sub_rec_end_idx]
        return []


    #
    # Given transaction type, transaction segments, and delim info, build out claims in the transaction
    #  @return a list of Claim for each "clm" segment
    #
    def build(self):
        if self.trnx_cls.NAME in ['837I', '837P']:
            return [
                self.build_claim(seg, i) for i, seg in self.segments_by_name_index("CLM")
            ]
        elif self.trnx_cls.NAME == '835':
            return [
                self.build_remittance(seg, i) for i, seg in self.segments_by_name_index("CLP")
            ]
        elif self.trnx_cls.NAME == '834':
            return [
                self.build_enrollment(seg, i) for i, seg in self.segments_by_name_index("BGN")
            ]

#
# Base claim class
#

class MedicalClaim(EDI):

    def __init__(
        self,
        sender_receiver_loop: List = [],
        billing_loop: List = [],
        subscriber_loop: List = [],
        patient_loop: List = [],
        claim_loop: List = [],
        sl_loop: List = [], 
    ):
        self.sender_receiver_loop = sender_receiver_loop # extracted together
        self.billing_loop = billing_loop
        self.subscriber_loop = subscriber_loop
        self.patient_loop = patient_loop
        self.claim_loop = claim_loop
        self.sl_loop = sl_loop
        self.build()

    #
    # Return first segment found of name == name otherwise Segment.empty()
    #
    def _first(self, segments, name, start_index = 0):
        return ([x for x in segments[start_index:] if x._name == name][0]  if len([x for x in segments[start_index:] if x._name == name]) > 0 else Segment.empty())
        
    def _populate_providers(self):
        return {"billing": self._billing_provider()}
    
    def _billing_provider(self):
        return ProviderIdentity(segments=self.billing_loop)

    def _populate_diagnosis(self):
        return DiagnosisIdentity(segments=self.claim_loop)
    
    def _populate_submitter_loop(self) -> Dict[str, str]:
        # Find the submitter NM1 (entity code 41)
        submitter_idx = -1
        for i, seg in enumerate(self.sender_receiver_loop):
            if seg._name == "NM1" and seg.element(1) == "41":
                submitter_idx = i
                break
        
        if submitter_idx == -1:
            return Submitter_Receiver_Identity(segments=[])
        
        # Slice until the next NM1 or end of loop
        next_nm1_idx = -1
        for i in range(submitter_idx + 1, len(self.sender_receiver_loop)):
            if self.sender_receiver_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.sender_receiver_loop)
        # GREEDY: Pass everything (NM1, PER, REF, etc.)
        return Submitter_Receiver_Identity(segments=self.sender_receiver_loop[submitter_idx:end_idx])
    
    def _populate_receiver_loop(self) -> Dict[str, str]:
        # Find the receiver NM1 (entity code 40)
        receiver_idx = -1
        for i, seg in enumerate(self.sender_receiver_loop):
            if seg._name == "NM1" and seg.element(1) == "40":
                receiver_idx = i
                break
        
        if receiver_idx == -1:
            return Submitter_Receiver_Identity(segments=[])
        
        # Slice until the next NM1 or end of loop
        next_nm1_idx = -1
        for i in range(receiver_idx + 1, len(self.sender_receiver_loop)):
            if self.sender_receiver_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.sender_receiver_loop)
        # GREEDY: Pass everything (NM1, PER, REF, etc.)
        return Submitter_Receiver_Identity(segments=self.sender_receiver_loop[receiver_idx:end_idx])

    def _populate_subscriber_loop(self):
        l = self.subscriber_loop[0:min(filter(lambda x: x!= -1, [self.index_of_segment(self.subscriber_loop, "CLM"), len(self.subscriber_loop)]))] #subset the subscriber loop before the CLM segment
        return PatientIdentity(segments=l)
    
    def _populate_patient_loop(self) -> Dict[str, str]:
        # Note - if this doesn't exist then it's the same as subscriber loop
        # 01 = Spouse; 18 = Self; 19 = Child; G8 = Other
        return self._populate_subscriber_loop() if self._first(self.subscriber_loop, "SBR").element(2) == "18" else PatientIdentity(segments=self.patient_loop)
    
    def _populate_claim_loop(self):
        return ClaimIdentity(segments=self.claim_loop)


                             

    def _populate_payer_info(self):
        # Find the Payer NM1 (entity code PR)
        payer_idx = -1
        for i, seg in enumerate(self.subscriber_loop):
            if seg._name == "NM1" and seg.element(1) == "PR":
                payer_idx = i
                break
        
        if payer_idx == -1:
            return PayerIdentity(segments=[])
        
        # Slice until the next NM1 or end of loop
        next_nm1_idx = -1
        for i in range(payer_idx + 1, len(self.subscriber_loop)):
            if self.subscriber_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.subscriber_loop)
        # GREEDY: Pass everything (N3, N4, REF, PER, etc.)
        return PayerIdentity(segments=self.subscriber_loop[payer_idx:end_idx])
    
    """
    Overall Asks
    - Coordination of Benefits flag -- > self.benefits_assign_flag in Claim Identity
    """
    def to_json(self):
        return {
            **{'submitter': self.submitter_info.to_dict()},
            **{'receiver': self.receiver_info.to_dict()},
            **{'subscriber': self.subscriber_info.to_dict()},
            **{'patient': self.patient_info.to_dict()},
            **{'payer': self.payer_info.to_dict()},
            **{'providers': {k:v.to_dict() for k,v in self.provider_info.items()}}, #returns a dictionary of k=provider type
            **{'claim_header': self.claim_info.to_dict()},
            **{'claim_lines': [x.to_dict() for x in self.sl_info]}, #List
            **{'diagnosis': self.diagnosis_info.to_dict()}
        }

    def _service_facility_provider(self):
        # Find index of service facility NM1 (entity code 77)
        facility_idx = -1
        for i, seg in enumerate(self.claim_loop):
            if seg._name == "NM1" and seg.element(1) == "77":
                facility_idx = i
                break
        
        if facility_idx == -1:
            return ProviderIdentity(segments=[])
        
        # Find the next NM1 index (or end of loop)
        next_nm1_idx = -1
        for i in range(facility_idx + 1, len(self.claim_loop)):
            if self.claim_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.claim_loop)
        # GREEDY: Pass everything (NM1, N3, N4, REF, PRV, etc.)
        return ProviderIdentity(segments=self.claim_loop[facility_idx:end_idx])

    #
    # Returns each claim line as an array of segments that make up the claim line
    #
    def claim_lines(self):
        return list(map(lambda i: self.sl_loop[i[0]:i[1]],
                self._index_to_tuples([(i) for i,y in enumerate(self.sl_loop) if y._name=="LX"]+[len(self.sl_loop)])))

    def build(self) -> None:
        self.submitter_info = self._populate_submitter_loop()
        self.receiver_info = self._populate_receiver_loop()
        self.subscriber_info = self._populate_subscriber_loop()
        self.patient_info = (
            self._populate_subscriber_loop() if self.patient_loop == [] else self._populate_patient_loop()
        )
        self.sl_info =  self._populate_sl_loop()
        self.claim_info = self._populate_claim_loop()
        self.provider_info = self._populate_providers()
        self.diagnosis_info = self._populate_diagnosis()
        self.payer_info = self._populate_payer_info()

class Claim837i(MedicalClaim):

    NAME = "837I"

    # Format for 837I https://www.dhs.wisconsin.gov/publications/p0/p00266.pdf
    
    def _attending_provider(self):
        # Find index of attending provider NM1 (entity code 71)
        attending_idx = -1
        for i, seg in enumerate(self.claim_loop):
            if seg._name == "NM1" and seg.element(1) == "71":
                attending_idx = i
                break
        
        if attending_idx == -1:
            return ProviderIdentity(segments=[])
        
        # Find the next NM1 index (or end of loop)
        next_nm1_idx = -1
        for i in range(attending_idx + 1, len(self.claim_loop)):
            if self.claim_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.claim_loop)
        # GREEDY: Pass everything (NM1, PRV, REF, N3, N4, etc.)
        return ProviderIdentity(segments=self.claim_loop[attending_idx:end_idx])

    def _operating_provider(self):
        # Find index of operating provider NM1 (entity code 72)
        operating_idx = -1
        for i, seg in enumerate(self.claim_loop):
            if seg._name == "NM1" and seg.element(1) == "72":
                operating_idx = i
                break
        
        if operating_idx == -1:
            return ProviderIdentity(segments=[])
        
        # Find the next NM1 index (or end of loop)
        next_nm1_idx = -1
        for i in range(operating_idx + 1, len(self.claim_loop)):
            if self.claim_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.claim_loop)
        # GREEDY: Pass everything (NM1, PRV, REF, N3, N4, etc.)
        return ProviderIdentity(segments=self.claim_loop[operating_idx:end_idx])

    def _other_provider(self):
        # Find index of other provider NM1 (entity code 73)
        other_idx = -1
        for i, seg in enumerate(self.claim_loop):
            if seg._name == "NM1" and seg.element(1) == "73":
                other_idx = i
                break
        
        if other_idx == -1:
            return ProviderIdentity(segments=[])
        
        # Find the next NM1 index (or end of loop)
        next_nm1_idx = -1
        for i in range(other_idx + 1, len(self.claim_loop)):
            if self.claim_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.claim_loop)
        # GREEDY: Pass everything (NM1, PRV, REF, N3, N4, etc.)
        return ProviderIdentity(segments=self.claim_loop[other_idx:end_idx]) 
      
    def _populate_providers(self):
        return {"billing": self._billing_provider(),
                "attending": self._attending_provider(),
                "operating": self._operating_provider(),
                "other": self._other_provider(),
                "service_facility": self._service_facility_provider()
                }

    def _populate_claim_loop(self):
        return ClaimIdentity(segments=self.claim_loop)

    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv2(
                    segments=s,  # <--- NEW: Pass full raw segments
                    sv2 = self._first(s, "SV2"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data = s),
                    amt = self.segments_by_name("AMT", data=s)
                ),self.claim_lines()))

    
class Claim837p(MedicalClaim):

    NAME = "837P"
    # Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf

    def _rendering_provider(self):
        # Find index of rendering provider NM1 (entity code 82)
        rendering_idx = -1
        for i, seg in enumerate(self.claim_loop):
            if seg._name == "NM1" and seg.element(1) == "82":
                rendering_idx = i
                break
        
        if rendering_idx == -1:
            return ProviderIdentity(segments=[])
        
        # Find the next NM1 index (or end of loop)
        next_nm1_idx = -1
        for i in range(rendering_idx + 1, len(self.claim_loop)):
            if self.claim_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.claim_loop)
        # GREEDY: Pass everything (NM1, PRV, REF, N3, N4, etc.)
        return ProviderIdentity(segments=self.claim_loop[rendering_idx:end_idx])
    
    def _referring_provider(self):
        # Find index of referring provider NM1 (entity code DN)
        referring_idx = -1
        for i, seg in enumerate(self.claim_loop):
            if seg._name == "NM1" and seg.element(1) == "DN":
                referring_idx = i
                break
        
        if referring_idx == -1:
            return ProviderIdentity(segments=[])
        
        # Find the next NM1 index (or end of loop)
        next_nm1_idx = -1
        for i in range(referring_idx + 1, len(self.claim_loop)):
            if self.claim_loop[i]._name == "NM1":
                next_nm1_idx = i
                break
        
        end_idx = next_nm1_idx if next_nm1_idx != -1 else len(self.claim_loop)
        # GREEDY: Pass everything (NM1, PRV, REF, N3, N4, etc.)
        return ProviderIdentity(segments=self.claim_loop[referring_idx:end_idx])


    def _populate_providers(self):
        return {"billing": self._billing_provider(),
                "referring": self._referring_provider(),
                "servicing": (self._billing_provider() if self._rendering_provider() is None else self._rendering_provider()),
                "service_facility": self._service_facility_provider()
                }

    
    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine.from_sv1(
                    segments=s,  # <--- NEW: Pass full raw segments
                    sv1 = self._first(s, "SV1"),
                    lx = self._first(s, "LX"),
                    dtp = self.segments_by_name("DTP", data=s),
                    amt = self.segments_by_name("AMT", data=s)
                ), self.claim_lines()))
