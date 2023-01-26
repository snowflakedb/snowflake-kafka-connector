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

package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class IngestSdkProviderTest {
    private Map<String, String> config;
    private String connectorName;

    @Before
    // set up sunny day tests
    public void setup() {
        this.connectorName = "testConnector";

        // config
        this.config = TestUtils.getConfForStreaming();
        SnowflakeSinkConnectorConfig.setDefaultValues(this.config);
    }

    @Test
    public void testCreateClient() {
        // setup
        IngestSdkProvider ingestSdkProvider = new IngestSdkProvider();
        SnowflakeStreamingIngestClient goalClient = TestUtils.createStreamingClient(this.config, "KC_CLIENT_" + this.connectorName + "0");

        // test
        SnowflakeStreamingIngestClient createdClient = ingestSdkProvider.createStreamingClient(this.config, this.connectorName);

        // verification - very difficult (impossible?) to mock the SnowflakeStreamingIngestClientFactory.builder method because it is static, so use a goal client to at least test idempotency
        assert createdClient.getName().equals(goalClient.getName());
    }

    @Test
    public void testCloseClient() throws Exception {
        // setup
        IngestSdkProvider ingestSdkProvider = new IngestSdkProvider();
        SnowflakeStreamingIngestClient createdClient = ingestSdkProvider.createStreamingClient(config, connectorName);
        SnowflakeStreamingIngestClient goalClient = TestUtils.createStreamingClient(this.config, "KC_CLIENT_" + this.connectorName + "0");
        goalClient.close();

        // test
        ingestSdkProvider.closeStreamingClient();

        // verify
        assert createdClient.isClosed() == goalClient.isClosed();
    }

    @Test
    public void testGetClient() {
        // setup
        IngestSdkProvider ingestSdkProvider = new IngestSdkProvider();
        ingestSdkProvider.createStreamingClient(config, connectorName);
        SnowflakeStreamingIngestClient goalClient = TestUtils.createStreamingClient(this.config, "KC_CLIENT_" + this.connectorName + "0");

        // test
        SnowflakeStreamingIngestClient gotClient = ingestSdkProvider.getStreamingIngestClient();

        // verify
        assert gotClient.getName().equals(goalClient.getName());
    }

    @Test
    public void testGetClientFailure() {
        IngestSdkProvider ingestSdkProvider = new IngestSdkProvider();
        assert TestUtils.assertError(SnowflakeErrors.ERROR_3009, () -> { ingestSdkProvider.getStreamingIngestClient(); });
    }

    @Test(expected = ConnectException.class)
    public void testMissingPropertiesForStreamingClient() {
        this.config.remove(Utils.SF_ROLE);
        IngestSdkProvider ingestSdkProvider = new IngestSdkProvider();

        try {
            ingestSdkProvider.createStreamingClient(this.config, connectorName);
        } catch(ConnectException ex) {
            assert ex.getCause() instanceof SFException;
            assert ex.getCause().getMessage().contains("Missing role");
            throw ex;
        }
    }
}
