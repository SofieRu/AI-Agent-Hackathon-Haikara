import hashlib
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import serialization

class CryptoUtils:
    """Cryptographic utilities for Beckn signatures"""
    
    @staticmethod
    def generate_key_pair():
        """Generate RSA key pair for Beckn signing"""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        
        public_key = private_key.public_key()
        
        return private_key, public_key
    
    @staticmethod
    def sign_payload(payload: str, private_key) -> str:
        """Sign payload with private key"""
        signature = private_key.sign(
            payload.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode()
    
    @staticmethod
    def verify_signature(payload: str, signature: str, public_key) -> bool:
        """Verify signature with public key"""
        try:
            public_key.verify(
                base64.b64decode(signature),
                payload.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except:
            return False
    
    @staticmethod
    def hash_data(data: str) -> str:
        """Create SHA-256 hash"""
        return hashlib.sha256(data.encode()).hexdigest()