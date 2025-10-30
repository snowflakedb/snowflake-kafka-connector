/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Embedded HTTP/HTTPS proxy server for testing proxy configurations. Uses Testcontainers with a
 * Squid proxy to create a production-grade proxy server that runs in Docker.
 *
 * <p>This implementation uses the same squid.conf file as the CI pipeline
 * (.github/scripts/squid.conf) to ensure consistent behavior between local tests and CI
 * environment. The proxy requires basic authentication with username/password.
 *
 * <p>Can be used as a JUnit Rule to automatically manage the proxy lifecycle per test method,
 * ensuring parallel test execution without port conflicts.
 */
public class EmbeddedProxyServer extends ExternalResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedProxyServer.class);
  private static final int SQUID_PORT = 3128;
  private static final String SQUID_CONF_RESOURCE = "squid.conf";

  private final String username;
  private final String password;
  private GenericContainer<?> proxyContainer;
  private Path tempSquidConfFile;

  public EmbeddedProxyServer(final String username, final String password) {
    this.username = username;
    this.password = password;
  }

  /**
   * Starts the proxy server. The server uses Squid proxy with basic authentication matching the CI
   * pipeline configuration. A random host port is mapped to the container's proxy port.
   *
   * <p>The proxy is configured using squid.conf loaded from test resources and creates a password
   * file compatible with squid's basic_ncsa_auth module.
   *
   * @throws IllegalStateException if the server is already running
   * @throws RuntimeException if the server fails to start or configuration files are missing
   */
  @SuppressWarnings("resource") // Container is managed and closed in stop() method
  public final void start() {
    if (proxyContainer != null) {
      throw new IllegalStateException("Proxy server is already running");
    }

    LOGGER.info(
        "Starting Squid proxy server with configuration from {} (verbose logging enabled)",
        SQUID_CONF_RESOURCE);

    try {
      // Load squid.conf from classpath resources
      tempSquidConfFile = loadSquidConfFromResources();

      LOGGER.info("Enabling verbose Squid logging - all proxy requests will be logged");

      // Use official squid image from Docker Hub
      // Container is closed in stop() method
      proxyContainer =
          new GenericContainer<>(DockerImageName.parse("sameersbn/squid:3.5.27-2"))
              .withExposedPorts(SQUID_PORT)
              .withCopyFileToContainer(
                  MountableFile.forHostPath(tempSquidConfFile.toAbsolutePath().toString()),
                  "/etc/squid/squid.conf")
              .withEnv("SQUID_CONFIG_FILE", "/etc/squid/squid.conf")
              .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("Squid")))
              .waitingFor(Wait.forListeningPort())
              .withStartupTimeout(Duration.ofSeconds(60));

      proxyContainer.start();

      LOGGER.info(
          "Squid proxy server started on localhost:{}, configuring authentication...",
          proxyContainer.getMappedPort(SQUID_PORT));

      // Generate password file INSIDE the container using the container's htpasswd
      // This ensures compatibility with the container's basic_ncsa_auth helper
      configureAuthentication();

      LOGGER.info(
          "Squid proxy server ready on localhost:{} with authentication (user: {})",
          proxyContainer.getMappedPort(SQUID_PORT),
          username);
      LOGGER.info("Proxy endpoint: localhost:{}", proxyContainer.getMappedPort(SQUID_PORT));
    } catch (Exception e) {
      LOGGER.error("Failed to start proxy server", e);
      cleanup();
      throw new RuntimeException("Failed to start proxy server: " + e.getMessage(), e);
    }
  }

  /**
   * Loads the squid.conf file from classpath resources and copies it to a temporary file.
   * Testcontainers requires a file path to copy into the container.
   *
   * @return Path to the temporary squid.conf file
   * @throws IOException if the resource cannot be read or temporary file cannot be created
   */
  private Path loadSquidConfFromResources() throws IOException {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(SQUID_CONF_RESOURCE)) {
      if (is == null) {
        throw new RuntimeException(
            "Squid configuration file not found in classpath resources: " + SQUID_CONF_RESOURCE);
      }

      Path tempFile = Files.createTempFile("squid-", ".conf");
      Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
      LOGGER.debug("Loaded squid.conf from resources to temporary file: {}", tempFile);
      return tempFile;
    }
  }

  /**
   * Configures authentication in the running Squid container by generating the password file using
   * the container's own htpasswd utility. This ensures compatibility with the container's
   * basic_ncsa_auth helper.
   *
   * @throws Exception if authentication configuration fails
   */
  private void configureAuthentication() throws Exception {
    LOGGER.info("Installing apache2-utils and generating password file");
    try {
      LOGGER.debug("Installing apache2-utils package...");
      var installResult =
          proxyContainer.execInContainer(
              "sh",
              "-c",
              "apt-get update -qq && apt-get install -y -qq apache2-utils 2>&1 | tail -5");
      if (installResult.getExitCode() != 0) {
        LOGGER.error("Failed to install apache2-utils: {}", installResult.getStderr());
        throw new RuntimeException("Could not install apache2-utils");
      }
      LOGGER.debug("apache2-utils installed successfully");
    } catch (Exception e) {
      LOGGER.error("Failed to install apache2-utils", e);
      throw new RuntimeException("Failed to install apache2-utils: " + e.getMessage(), e);
    }

    // Create password file using container's htpasswd (ensures compatibility)
    try {
      LOGGER.info("Generating password file using htpasswd");
      var result =
          proxyContainer.execInContainer(
              "htpasswd", "-bc", "/etc/squid/passwords", username, password);
      LOGGER.debug("Password file created. Exit code: {}", result.getExitCode());
      if (result.getExitCode() != 0) {
        LOGGER.error("htpasswd failed: {}", result.getStderr());
        throw new RuntimeException("htpasswd failed with exit code: " + result.getExitCode());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to create password file in container", e);
      throw new RuntimeException("Failed to create password file: " + e.getMessage(), e);
    }

    // Verify password file permissions
    try {
      proxyContainer.execInContainer("chmod", "644", "/etc/squid/passwords");
      LOGGER.debug("Password file permissions set to 644");
    } catch (Exception e) {
      LOGGER.warn("Could not set password file permissions: {}", e.getMessage());
    }

    // Restart squid to pick up the new password file
    LOGGER.info("Restarting Squid to apply authentication configuration");
    try {
      proxyContainer.execInContainer("supervisorctl", "restart", "squid");
      LOGGER.debug("Squid restart command sent");

      // Wait for squid to restart and be ready
      Thread.sleep(3000);
      LOGGER.debug("Authentication configuration complete");
    } catch (Exception e) {
      LOGGER.warn(
          "Could not restart via supervisorctl, squid may pick up config automatically: {}",
          e.getMessage());
      // Continue anyway - squid might reload automatically or on next request
    }
  }

  private void cleanup() {
    if (proxyContainer != null) {
      try {
        proxyContainer.stop();
      } catch (Exception stopEx) {
        LOGGER.warn("Error stopping container during cleanup", stopEx);
      }
      proxyContainer = null;
    }

    if (tempSquidConfFile != null) {
      try {
        Files.deleteIfExists(tempSquidConfFile);
        LOGGER.debug("Deleted temporary squid.conf file: {}", tempSquidConfFile);
      } catch (IOException e) {
        LOGGER.warn("Failed to delete temporary squid.conf file: {}", tempSquidConfFile, e);
      }
      tempSquidConfFile = null;
    }
  }

  /**
   * Stops the proxy server and cleans up all resources including temporary files.
   *
   * @throws IllegalStateException if the server is not running
   */
  public final void stop() {
    if (proxyContainer == null) {
      throw new IllegalStateException("Proxy server is not running");
    }

    LOGGER.info("Stopping Squid proxy server (Container ID: {})", proxyContainer.getContainerId());

    LOGGER.info("================================================================================");
    LOGGER.info("END OF PROXY LOGS");
    LOGGER.info("================================================================================");

    try {
      cleanup();
      LOGGER.info("Squid proxy server stopped successfully");
    } catch (Exception e) {
      LOGGER.error("Error stopping proxy server", e);
      cleanup();
    }
  }

  public final boolean isRunning() {
    return proxyContainer != null && proxyContainer.isRunning();
  }

  public final int getPort() {
    if (proxyContainer == null) {
      throw new IllegalStateException("Proxy server is not running");
    }
    return proxyContainer.getMappedPort(SQUID_PORT);
  }

  public final String getUsername() {
    return username;
  }

  public final String getPassword() {
    return password;
  }

  @Override
  protected final void before() {
    start();
  }

  @Override
  protected final void after() {
    if (isRunning()) {
      stop();
    }
  }
}
