/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
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

package com.snowflake.kafka.connector.internal.streaming;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION;
import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;

public class StreamingClientProvider {
    public static final StreamingClientProvider streamingClientProvider = new StreamingClientProvider();

    private SnowflakeStreamingIngestClient streamingIngestClient;
    private Map<String, String> connectorConfig;

    // for logging purposes
    private int numCreatedClients;

    // private constructor for singleton
    private StreamingClientProvider() {
        numCreatedClients = 0;
        connectorConfig = new HashMap<>();
    }

    public void createClient(Map<String, String> connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.streamingIngestClient = this.initStreamingClient();
        this.numCreatedClients++;
    }
    public void closeClient() {

    }

    public SnowflakeStreamingIngestClient getClient() {

        if (!this.isClientValid(this.streamingIngestClient)) {
            this.streamingIngestClient = this.initStreamingClient();
        }
    }

    private boolean isClientValid(SnowflakeStreamingIngestClient client) {
        return client != null && !client.isClosed();
    }

    private SnowflakeStreamingIngestClient initStreamingClient() {
        // get streaming properties from config
        Properties streamingClientProps = new Properties();
        streamingClientProps.putAll(StreamingUtils.convertConfigForStreamingClient(new HashMap<>(this.connectorConfig)));

            try {
                // Override only if bdec version is explicitly set in config, default to the version set
                // inside
                // Ingest SDK
                Map<String, Object> parameterOverrides = new HashMap<>();
                Optional<String> snowpipeStreamingBdecVersion =
                        Optional.ofNullable(this.connectorConfig.get(SNOWPIPE_STREAMING_FILE_VERSION));
                snowpipeStreamingBdecVersion.ifPresent(
                        overriddenValue -> {
                            LOGGER.info("Config is overridden for {} ", SNOWPIPE_STREAMING_FILE_VERSION);
                            parameterOverrides.put(BLOB_FORMAT_VERSION, overriddenValue);
                        });

                LOGGER.info("Initializing Streaming Client. ClientName:{}", this.streamingIngestClientName);
                this.streamingIngestClient =
                        SnowflakeStreamingIngestClientFactory.builder(this.streamingIngestClientName)
                                .setProperties(streamingClientProps)
                                .setParameterOverrides(parameterOverrides)
                                .build();
            } catch (SFException ex) {
                LOGGER.error(
                        "Exception creating streamingIngestClient with name:{}",
                        this.streamingIngestClientName);
                throw new ConnectException(ex);
            }

    }

    /** Closes the streaming client. */
    private void closeStreamingClient() {
        LOGGER.info("Closing Streaming Client:{}", this.streamingIngestClientName);
        try {
            streamingIngestClient.close();
        } catch (Exception e) {
            LOGGER.error(
                    "Failure closing Streaming client msg:{}, cause:{}",
                    e.getMessage(),
                    Arrays.toString(e.getCause().getStackTrace()));
        }
    }
}
