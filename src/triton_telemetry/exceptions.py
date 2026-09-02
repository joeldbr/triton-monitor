
from __future__ import annotations

class TritonError(Exception):

class ProviderTimeoutError(TritonError):

class CorruptedPayloadError(TritonError):
    
class NetworkPeeringError(TritonError):
    
