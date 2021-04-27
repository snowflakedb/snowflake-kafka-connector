package com.snowflake.kafka.connector.internal;

import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.Security;
import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;
import org.junit.Test;

public class FIPSTest {
  @Test
  public void testFips() throws IOException, OperatorCreationException {
    PrivateKey key = InternalUtils.parsePrivateKey(TestUtils.getKeyString());
    String password = "sfdsfs1312AAAFDSf121!!!";
    String AESKey = generateAESKey(key, password.toCharArray());
    // since bc-fips doesn't support encrypt rsa private key with DES,
    // load test key from test profile
    String DESKey = TestUtils.getDesRsaKey();
    // all key works by default
    EncryptionUtils.parseEncryptedPrivateKey(AESKey, password);
    EncryptionUtils.parseEncryptedPrivateKey(DESKey, password);

    // turn on approved only mode
    CryptoServicesRegistrar.setApprovedOnlyMode(true);
    // AES works
    EncryptionUtils.parseEncryptedPrivateKey(AESKey, password);
    // DES doesn't work
    TestUtils.assertError(
        SnowflakeErrors.ERROR_0018,
        () -> EncryptionUtils.parseEncryptedPrivateKey(DESKey, password));
  }

  public static String generateAESKey(PrivateKey key, char[] passwd)
      throws IOException, OperatorCreationException {
    Security.addProvider(new BouncyCastleFipsProvider());
    StringWriter writer = new StringWriter();
    JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    PKCS8EncryptedPrivateKeyInfoBuilder pkcs8EncryptedPrivateKeyInfoBuilder =
        new JcaPKCS8EncryptedPrivateKeyInfoBuilder(key);
    pemWriter.writeObject(
        pkcs8EncryptedPrivateKeyInfoBuilder.build(
            new JcePKCSPBEOutputEncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC)
                .setProvider("BCFIPS")
                .build(passwd)));
    pemWriter.close();
    return writer.toString();
  }
}
