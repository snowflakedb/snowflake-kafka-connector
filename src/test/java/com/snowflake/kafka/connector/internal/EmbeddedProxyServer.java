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
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight in-process HTTP CONNECT proxy for testing proxy configurations. Supports basic
 * authentication and HTTPS tunneling via the CONNECT method.
 *
 * <p>Can be used as a JUnit Rule to automatically manage the proxy lifecycle per test method.
 */
public class EmbeddedProxyServer extends ExternalResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedProxyServer.class);

  private final String username;
  private final String password;
  private ServerSocket serverSocket;
  private ExecutorService executor;
  private volatile boolean running;

  public EmbeddedProxyServer(final String username, final String password) {
    this.username = username;
    this.password = password;
  }

  public final void start() {
    if (serverSocket != null) {
      throw new IllegalStateException("Proxy server is already running");
    }

    try {
      serverSocket = new ServerSocket(0); // random available port
      running = true;
      executor =
          Executors.newCachedThreadPool(
              r -> {
                Thread t = new Thread(r, "proxy-worker");
                t.setDaemon(true);
                return t;
              });

      Thread acceptThread = new Thread(this::acceptLoop, "proxy-accept");
      acceptThread.setDaemon(true);
      acceptThread.start();

      LOGGER.info("Proxy server started on localhost:{}", serverSocket.getLocalPort());
    } catch (IOException e) {
      throw new RuntimeException("Failed to start proxy server: " + e.getMessage(), e);
    }
  }

  private void acceptLoop() {
    while (running) {
      try {
        Socket client = serverSocket.accept();
        executor.submit(() -> handleClient(client));
      } catch (IOException e) {
        if (running) {
          LOGGER.warn("Accept failed: {}", e.getMessage());
        }
      }
    }
  }

  private void handleClient(Socket client) {
    try {
      client.setSoTimeout(300_000);
      InputStream in = client.getInputStream();
      OutputStream out = client.getOutputStream();

      // Read the request line and headers
      String requestLine = readLine(in);
      if (requestLine == null) {
        client.close();
        return;
      }
      LOGGER.debug("Proxy request: {}", requestLine);

      String proxyAuth = null;
      String line;
      while ((line = readLine(in)) != null && !line.isEmpty()) {
        if (line.toLowerCase().startsWith("proxy-authorization:")) {
          proxyAuth = line.substring("proxy-authorization:".length()).trim();
        }
      }

      // Check authentication
      if (!checkAuth(proxyAuth)) {
        String response =
            "HTTP/1.1 407 Proxy Authentication Required\r\n"
                + "Proxy-Authenticate: Basic realm=\"proxy\"\r\n"
                + "Content-Length: 0\r\n\r\n";
        out.write(response.getBytes(StandardCharsets.US_ASCII));
        out.flush();
        client.close();
        return;
      }

      // Handle CONNECT (HTTPS tunneling)
      if (requestLine.startsWith("CONNECT ")) {
        handleConnect(requestLine, client, out);
      } else {
        // For non-CONNECT, just close — tests only need CONNECT for Snowflake HTTPS
        String response = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
        out.write(response.getBytes(StandardCharsets.US_ASCII));
        out.flush();
        client.close();
      }
    } catch (Exception e) {
      LOGGER.debug("Client handler error: {}", e.getMessage());
      try {
        client.close();
      } catch (IOException ignored) {
      }
    }
  }

  private void handleConnect(String requestLine, Socket client, OutputStream clientOut)
      throws IOException {
    // Parse "CONNECT host:port HTTP/1.1"
    String[] parts = requestLine.split(" ");
    if (parts.length < 2) {
      String response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
      clientOut.write(response.getBytes(StandardCharsets.US_ASCII));
      clientOut.flush();
      client.close();
      return;
    }
    String[] hostPort = parts[1].split(":");
    String host = hostPort[0];
    int port;
    try {
      port = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : 443;
    } catch (NumberFormatException e) {
      String response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
      clientOut.write(response.getBytes(StandardCharsets.US_ASCII));
      clientOut.flush();
      client.close();
      return;
    }

    try {
      Socket remote = new Socket(host, port);
      // Send 200 to client
      clientOut.write(
          "HTTP/1.1 200 Connection Established\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
      clientOut.flush();

      // Bidirectional relay
      Thread toRemote = new Thread(() -> relay(client, remote), "proxy-to-remote");
      toRemote.setDaemon(true);
      toRemote.start();
      relay(remote, client);

      toRemote.join(5000);
      toRemote.interrupt();
      remote.close();
    } catch (Exception e) {
      String response = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
      clientOut.write(response.getBytes(StandardCharsets.US_ASCII));
      clientOut.flush();
    }
    client.close();
  }

  private static void relay(Socket from, Socket to) {
    try {
      InputStream in = from.getInputStream();
      OutputStream out = to.getOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
        out.flush();
      }
    } catch (IOException ignored) {
      // Connection closed
    }
  }

  private boolean checkAuth(String proxyAuth) {
    if (proxyAuth == null) return false;
    if (!proxyAuth.startsWith("Basic ")) return false;
    String decoded =
        new String(Base64.getDecoder().decode(proxyAuth.substring(6)), StandardCharsets.UTF_8);
    return decoded.equals(username + ":" + password);
  }

  private static String readLine(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    int c;
    while ((c = in.read()) != -1) {
      if (c == '\r') {
        int next = in.read(); // consume \n
        if (next != '\n' && next != -1) {
          sb.append((char) c);
          sb.append((char) next);
          continue;
        }
        break;
      }
      if (c == '\n') break;
      sb.append((char) c);
    }
    return c == -1 && sb.length() == 0 ? null : sb.toString();
  }

  public final void stop() {
    if (serverSocket == null) {
      throw new IllegalStateException("Proxy server is not running");
    }

    LOGGER.info("Stopping proxy server on port {}", serverSocket.getLocalPort());
    running = false;
    try {
      serverSocket.close();
    } catch (IOException e) {
      LOGGER.warn("Error closing server socket", e);
    }
    serverSocket = null;

    if (executor != null) {
      executor.shutdownNow();
      try {
        executor.awaitTermination(2, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
      executor = null;
    }
    LOGGER.info("Proxy server stopped");
  }

  public final boolean isRunning() {
    return serverSocket != null && !serverSocket.isClosed();
  }

  public final int getPort() {
    if (serverSocket == null) {
      throw new IllegalStateException("Proxy server is not running");
    }
    return serverSocket.getLocalPort();
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
