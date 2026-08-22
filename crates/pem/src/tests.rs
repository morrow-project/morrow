use super::*;

#[test]
fn certificates_reject_empty_malformed_and_trailing_material() {
    assert!(certificates_from_slice(b"").is_err());
    assert!(
        certificates_from_slice(b"-----BEGIN CERTIFICATE-----\n%%%\n-----END CERTIFICATE-----\n")
            .is_err()
    );
    assert!(
        certificates_from_slice(
            b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\ntrailing"
        )
        .is_err()
    );
}

#[test]
fn private_keys_accept_pkcs8_and_legacy_encodings_but_reject_ambiguity() {
    let pkcs8 =
        private_key_from_slice(b"-----BEGIN PRIVATE KEY-----\nAQID\n-----END PRIVATE KEY-----\n")
            .unwrap();
    assert!(matches!(pkcs8, PrivateKeyDer::Pkcs8(_)));
    let pkcs1 = private_key_from_slice(
        b"-----BEGIN RSA PRIVATE KEY-----\nAQID\n-----END RSA PRIVATE KEY-----\n",
    )
    .unwrap();
    assert!(matches!(pkcs1, PrivateKeyDer::Pkcs1(_)));
    let sec1 = private_key_from_slice(
        b"-----BEGIN EC PRIVATE KEY-----\nAQID\n-----END EC PRIVATE KEY-----\n",
    )
    .unwrap();
    assert!(matches!(sec1, PrivateKeyDer::Sec1(_)));

    let multiple = b"-----BEGIN PRIVATE KEY-----\nAQID\n-----END PRIVATE KEY-----\n-----BEGIN PRIVATE KEY-----\nBAUG\n-----END PRIVATE KEY-----\n";
    assert!(private_key_from_slice(multiple).is_err());
    assert!(private_key_from_slice(b"").is_err());
}
