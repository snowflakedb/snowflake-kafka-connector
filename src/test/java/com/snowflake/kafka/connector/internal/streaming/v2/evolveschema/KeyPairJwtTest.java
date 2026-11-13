package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.time.Instant;
import java.util.Base64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class KeyPairJwtTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.ofEpochSecond(1_700_000_000L);

  private static KeyPair keyPair;

  @BeforeAll
  static void generateKey() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    keyPair = generator.generateKeyPair();
  }

  @Test
  void claimsMatchSnowflakeKeyPairFormat() throws Exception {
    String jwt =
        new KeyPairJwt("myacct.us-east-1", "kafka_user", "INGEST", keyPair.getPrivate())
            .create(NOW);

    String[] parts = jwt.split("\\.");
    assertThat(parts).hasSize(3);
    JsonNode header = decode(parts[0]);
    assertThat(header.get("alg").asText()).isEqualTo("RS256");
    assertThat(header.get("typ").asText()).isEqualTo("JWT");

    String fingerprint =
        Base64.getEncoder()
            .encodeToString(
                MessageDigest.getInstance("SHA-256").digest(keyPair.getPublic().getEncoded()));
    JsonNode claims = decode(parts[1]);
    assertThat(claims.get("iss").asText()).isEqualTo("MYACCT.KAFKA_USER.SHA256:" + fingerprint);
    assertThat(claims.get("sub").asText()).isEqualTo("MYACCT.KAFKA_USER");
    assertThat(claims.get("scp").asText()).isEqualTo("session:role:INGEST");
    assertThat(claims.get("iat").asLong()).isEqualTo(NOW.getEpochSecond());
    assertThat(claims.get("exp").asLong())
        .isEqualTo(NOW.plus(KeyPairJwt.LIFETIME).getEpochSecond());
  }

  @Test
  void signatureVerifiesWithPublicKey() throws Exception {
    String jwt = new KeyPairJwt("myacct", "u", null, keyPair.getPrivate()).create(NOW);
    int lastDot = jwt.lastIndexOf('.');

    Signature verifier = Signature.getInstance("SHA256withRSA");
    verifier.initVerify(keyPair.getPublic());
    verifier.update(jwt.substring(0, lastDot).getBytes(StandardCharsets.US_ASCII));
    assertThat(verifier.verify(Base64.getUrlDecoder().decode(jwt.substring(lastDot + 1)))).isTrue();
  }

  @Test
  void noRoleOmitsScopeClaim() throws Exception {
    String jwt = new KeyPairJwt("myacct", "u", " ", keyPair.getPrivate()).create(NOW);

    assertThat(decode(jwt.split("\\.")[1]).has("scp")).isFalse();
  }

  private static JsonNode decode(String part) throws Exception {
    return MAPPER.readTree(Base64.getUrlDecoder().decode(part));
  }
}
