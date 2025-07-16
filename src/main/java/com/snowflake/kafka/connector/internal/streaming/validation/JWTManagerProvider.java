package com.snowflake.kafka.connector.internal.streaming.validation;

import static com.snowflake.kafka.connector.internal.InternalUtils.parsePrivateKey;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_1006;
import static net.snowflake.ingest.utils.Utils.createKeyPairFromPrivateKey;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.EncryptionUtils;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.Optional;
import net.snowflake.ingest.connection.JWTManager;

/**
 * Build JWTManager class instance from the connector config. JWTManager is reused from
 * snowflake-ingest-java.
 */
public class JWTManagerProvider {

  public static JWTManager fromConfig(Map<String, String> config) {
    SnowflakeURL snowflakeURL = new SnowflakeURL(config.get(Utils.SF_URL));
    String rawPrivateKey = config.get(Utils.SF_PRIVATE_KEY);
    Optional<String> privateKeyPassphrase =
        Optional.ofNullable(config.get(Utils.PRIVATE_KEY_PASSPHRASE));
    PrivateKey privateKey =
        privateKeyPassphrase
            .map(passphrase -> EncryptionUtils.parseEncryptedPrivateKey(rawPrivateKey, passphrase))
            .orElseGet(() -> parsePrivateKey(rawPrivateKey));
    KeyPair keyPair = getKeyPair(privateKey);
    return new JWTManager(snowflakeURL.getAccount(), Utils.getUser(config), keyPair, null);
  }

  private static KeyPair getKeyPair(PrivateKey privateKey) {
    try {
      return createKeyPairFromPrivateKey(privateKey);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw ERROR_1006.getException(e);
    }
  }
}
