from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


@dataclass(frozen=True, slots=True)
class CertificatePaths:
    ca_key: Path
    ca_cert: Path
    key: Path
    csr: Path
    cert: Path


@dataclass(frozen=True, slots=True)
class LocalCertificateAuthority:
    """Project-local PKI automation for mTLS-enabled development clusters."""

    directory: Path
    days: int = 365

    def bootstrap_ca(self, *, common_name: str = "distsequencer-local-ca") -> CertificatePaths:
        self.directory.mkdir(parents=True, exist_ok=True)
        paths = self.paths_for("ca")
        key = _new_key()
        subject = issuer = _name(common_name)
        now = datetime.now(UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + timedelta(days=self.days))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=True,
                    crl_sign=True,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .sign(key, hashes.SHA256())
        )
        _write_key(paths.ca_key, key)
        _write_cert(paths.ca_cert, cert)
        return paths

    def issue_node_certificate(self, node_id: str) -> CertificatePaths:
        paths = self.paths_for(node_id)
        if not paths.ca_key.exists() or not paths.ca_cert.exists():
            raise FileNotFoundError("CA is missing; call bootstrap_ca first")
        ca_key = serialization.load_pem_private_key(paths.ca_key.read_bytes(), password=None)
        if not isinstance(ca_key, rsa.RSAPrivateKey):
            raise TypeError("CA private key must be RSA")
        ca_cert = x509.load_pem_x509_certificate(paths.ca_cert.read_bytes())
        key = _new_key()
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(_name(node_id))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(node_id)]), critical=False)
            .sign(key, hashes.SHA256())
        )
        now = datetime.now(UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(csr.subject)
            .issuer_name(ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + timedelta(days=self.days))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.ExtendedKeyUsage(
                    [
                        x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                        x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
                    ]
                ),
                critical=False,
            )
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(node_id)]), critical=False)
            .sign(ca_key, hashes.SHA256())
        )
        _write_key(paths.key, key)
        paths.csr.write_bytes(csr.public_bytes(serialization.Encoding.PEM))
        _write_cert(paths.cert, cert)
        return paths

    def paths_for(self, node_id: str) -> CertificatePaths:
        safe_id = "".join(char if char.isalnum() or char in "-_" else "_" for char in node_id)
        return CertificatePaths(
            ca_key=self.directory / "ca.key.pem",
            ca_cert=self.directory / "ca.cert.pem",
            key=self.directory / f"{safe_id}.key.pem",
            csr=self.directory / f"{safe_id}.csr.pem",
            cert=self.directory / f"{safe_id}.cert.pem",
        )


def _new_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=3072)


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
