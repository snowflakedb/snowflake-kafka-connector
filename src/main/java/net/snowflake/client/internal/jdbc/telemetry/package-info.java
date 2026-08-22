/**
 * TEMPORARY SHIM PACKAGE — not production code.
 *
 * <p>The classic {@code net.snowflake:snowflake-jdbc} driver exposed an in-band JDBC telemetry API
 * under {@code net.snowflake.client.internal.jdbc.telemetry} ({@link
 * net.snowflake.client.internal.jdbc.telemetry.Telemetry Telemetry}, {@link
 * net.snowflake.client.internal.jdbc.telemetry.TelemetryClient TelemetryClient}, {@link
 * net.snowflake.client.internal.jdbc.telemetry.TelemetryData TelemetryData}, {@link
 * net.snowflake.client.internal.jdbc.telemetry.TelemetryUtil TelemetryUtil}). The replacement
 * {@code net.snowflake:snowflake-jdbc-native} artifact (private preview) does not yet ship this
 * package, so the connector would not compile against it.
 *
 * <p>These minimal stand-ins let the connector build and run against snowflake-jdbc-native so the
 * rest of the driver can be exercised in CI. {@link
 * net.snowflake.client.internal.jdbc.telemetry.TelemetryClient#createTelemetry(java.sql.Connection)
 * createTelemetry} returns a no-op client, so connector-side telemetry is silently disabled until
 * the native driver provides a real implementation.
 *
 * <p>GAP: snowflake-jdbc-native must expose a JDBC telemetry API before connector telemetry works.
 * Delete this package once it does.
 */
package net.snowflake.client.internal.jdbc.telemetry;
