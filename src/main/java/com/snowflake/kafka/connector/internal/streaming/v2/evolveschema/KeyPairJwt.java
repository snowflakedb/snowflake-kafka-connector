package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Locale;

/**
 * Builds the key-pair JWT that Snowflake accepts as a user token, in the form the streaming SDK
 * sends.
 */
final class KeyPairJwt {

  static final Duration LIFETIME = Duration.ofMinutes(59);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();
  private static final String HEADER =
      BASE64_URL.encodeToString(
          "{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));

  private final String qualifiedUser;
  private final String issuer;
  private final String role;
  private final PrivateKey privateKey;

  /**
   * @param account account identifier; only the part before the first '.' is used
   * @param user login name of the key's user
   * @param role role to request in the {@code scp} claim; null or blank sends none
   * @param privateKey RSA private key whose public key is registered on {@code user}
   */
  KeyPairJwt(String account, String user, String role, PrivateKey privateKey) {
    String accountName = account.split("\\.", 2)[0].toUpperCase(Locale.ROOT);
    this.qualifiedUser = accountName + "." + user.toUpperCase(Locale.ROOT);
    this.issuer = qualifiedUser + ".SHA256:" + publicKeyFingerprint(privateKey);
    this.role = role;
    this.privateKey = privateKey;
  }

  /** Returns a JWT issued at {@code now}, valid for {@link #LIFETIME}. */
  String create(Instant now) {
    ObjectNode claims = MAPPER.createObjectNode();
    claims.put("iss", issuer);
    claims.put("sub", qualifiedUser);
    if (role != null && !role.isBlank()) {
      claims.put("scp", "session:role:" + role);
    }
    claims.put("iat", now.getEpochSecond());
    claims.put("exp", now.plus(LIFETIME).getEpochSecond());

    String signingInput =
        HEADER
            + "."
            + BASE64_URL.encodeToString(claims.toString().getBytes(StandardCharsets.UTF_8));
    try {
      Signature signature = Signature.getInstance("SHA256withRSA");
      signature.initSign(privateKey);
      signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
      return signingInput + "." + BASE64_URL.encodeToString(signature.sign());
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to sign key-pair JWT", e);
    }
  }

  /** Base64 SHA-256 of the DER-encoded public key, as Snowflake shows in RSA_PUBLIC_KEY_FP. */
  private static String publicKeyFingerprint(PrivateKey privateKey) {
    if (!(privateKey instanceof RSAPrivateCrtKey)) {
      throw new IllegalArgumentException(
          "Key-pair authentication needs an RSA private key that carries its public exponent");
    }
    RSAPrivateCrtKey rsaKey = (RSAPrivateCrtKey) privateKey;
    try {
      PublicKey publicKey =
          KeyFactory.getInstance("RSA")
              .generatePublic(
                  new RSAPublicKeySpec(rsaKey.getModulus(), rsaKey.getPublicExponent()));
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(publicKey.getEncoded());
      return Base64.getEncoder().encodeToString(digest);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to derive the public key fingerprint", e);
    }
  }
}
