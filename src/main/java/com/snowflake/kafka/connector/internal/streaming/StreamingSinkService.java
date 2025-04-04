package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.SnowflakeSinkService;

/**
 * A general base class for SnowpipeStreaming based services. This class should not depend on any V1
 * or V2 specifics.
 */
public abstract class StreamingSinkService implements SnowflakeSinkService {}
