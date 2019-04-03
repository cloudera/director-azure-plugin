/*
 * Copyright (c) 2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.director.azure.utils;

import static com.cloudera.director.spi.v2.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.cloudera.director.spi.v2.common.http.HttpProxyParameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.net.ssl.SSLSocketFactory;

/**
 * An SSLSocketFactory that uses HttpProxyParameters to describe a proxy with basic authorization.
 */
public class SSLTunnelSocketFactory extends SSLSocketFactory {

  private static final String USER_AGENT;

  private final SSLSocketFactory defaultFactory;
  private final HttpProxyParameters httpProxyParameters;

  static {
    // This was adapted from sun.net.www.protocol.http.HttpURLConnection, which is proprietary and not
    // guaranteed to be in OpenJDK. It's used to set the user agent during the proxy handshake and for
    // nothing else. I elected to keep this the same as the implementation in HttpURLConnection for
    // consistency's sake, though I don't think the content of the user agent really matters.

    String agent = System.getProperty("http.agent");
    String javaVersion = System.getProperty("java.version");
    if (agent == null) {
      agent = "Java/" + javaVersion;
    } else {
      agent = agent + " Java/" + javaVersion;
    }

    USER_AGENT = agent;
  }

  public SSLTunnelSocketFactory(HttpProxyParameters httpProxyParameters) {
    this.defaultFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
    this.httpProxyParameters = requireNonNull(httpProxyParameters, "httpProxyParameters is null");

    requireNonNull(this.httpProxyParameters.getHost(), "httpProxyParameters host is null");
    checkArgument(this.httpProxyParameters.getPort() > 0,
        "httpProxyParameters port must be greater than 0");
    requireNonNull(this.httpProxyParameters.getUsername(), "httpProxyParameters username is null");
    requireNonNull(this.httpProxyParameters.getPassword(), "httpProxyParameters password is null");
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return defaultFactory.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return defaultFactory.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
    return createSocket(null, host, port, true);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort)
      throws IOException, UnknownHostException {
    return createSocket(null, host, port, true);
  }

  @Override
  public Socket createSocket(InetAddress address, int port) throws IOException {
    return createSocket(null, address.getHostName(), port, true);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress clientAddress, int clientPort) throws IOException {
    return createSocket(null, address.getHostName(), port, true);
  }

  @Override
  public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
    Socket proxy = new Socket(httpProxyParameters.getHost(), httpProxyParameters.getPort());

    // Handshake with the proxy
    OutputStream proxyOutput = proxy.getOutputStream();

    String auth = "Basic " + Base64.getEncoder().encodeToString(
        (httpProxyParameters.getUsername() + ":" + httpProxyParameters.getPassword()).getBytes(StandardCharsets.UTF_8));
    String handshakeMessage = "CONNECT " + host + ":" + port + " HTTP/1.1\n" +
        "User-Agent: " + USER_AGENT + "\n" +
        "Proxy-Authorization: " + auth +
        "\r\n\r\n";

    proxyOutput.write(handshakeMessage.getBytes(StandardCharsets.UTF_8));
    proxyOutput.flush();
    // We don't want to close this input stream
    StringBuilder reply = new StringBuilder();
    int lines = 0;
    boolean headerComplete = false;

    InputStream proxyInput = proxy.getInputStream();

    while (lines < 2) {
      int inputChar = proxyInput.read();

      if (inputChar < 0) {
        throw new IOException("Unexpected EOF during proxy handshake");
      }

      switch(inputChar) {
        case '\n':
          headerComplete = true;
          lines++;
          break;
        case '\r':
          break; // Do nothing for \r
        default:
          lines = 0;
          if (!headerComplete) {
            reply.append((char) inputChar);
          }
          break;
      }
    }

    if (!reply.toString().toLowerCase().contains("200 connection established")) {
      throw new IOException("Unable to tunnel through proxy " + httpProxyParameters.getHost() + ":" +
          httpProxyParameters.getPort() + ". Reply was \"" + reply + "\"");
    }

    // Handshake complete, create socket as normal

    return defaultFactory.createSocket(proxy, host, port, autoClose);
  }
}
