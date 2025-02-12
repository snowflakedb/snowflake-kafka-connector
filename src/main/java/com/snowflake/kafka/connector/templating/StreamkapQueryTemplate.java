package com.snowflake.kafka.connector.templating;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class StreamkapQueryTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamkapQueryTemplate.class);
    private final ConcurrentMap<String, TopicConfig> allTopicConfigs = new ConcurrentHashMap<>();
    private Mustache createSqlTemplate = null;
    private Mustache tableNameTemplate = null;
    private Map<String, Object> createSqlData = null;
    private String sfWarehouse;
    private int targetLag = 15;
    private int cleanupTaskSchedule = 60;
    private long schemaChangeIntervalMs =0;
    private boolean isSFWarehouseExists = false;
    static MustacheFactory mustacheFactory = new DefaultMustacheFactory();
    private long schemaCheckTime;
    private boolean applyDynamicTableScript;
    private final ConcurrentHashMap<String, SinkRecord> recordByTopic = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> processedTopics = new ConcurrentHashMap<>();


    public StreamkapQueryTemplate() {
        // Private constructor to prevent instantiation
    }

    void setSFWarehouse (String sfWarehouse) {
        this.sfWarehouse = sfWarehouse;
        this.isSFWarehouseExists = (this.sfWarehouse != null && !this.sfWarehouse.trim().isEmpty());
    }

    public boolean isSFWarehouseExists () {
        return this.isSFWarehouseExists;
    }
    void setTargetLag(int targetLag) {
        this.targetLag = targetLag;
    }

    void setCleanupTaskSchedule(int cleanupTaskSchedule) {
        this.cleanupTaskSchedule = cleanupTaskSchedule;
    }

    void setSchemaChangeIntervalMs( long schemaChangeIntervalMs) {
        this.schemaChangeIntervalMs = schemaChangeIntervalMs;
    }

    void setApplyDynamicTableScript(boolean applyDynamicTableScript) {
        this.applyDynamicTableScript = applyDynamicTableScript;
    }

    public boolean isApplyDynamicTableScript() {
        return this.applyDynamicTableScript;
    }

    public long getSchemaChangeIntervalMs() {
        return this.schemaChangeIntervalMs;
    }

    public static StreamkapQueryTemplate buildStreamkapQueryTemplateFromConfig(final Map<String, String> parsedConfig) {
        String topics = parsedConfig.get(SnowflakeSinkConnectorConfig.TOPICS);
        String topicsMapping = parsedConfig.get(Utils.TOPICS_MAP_CONF);
        StreamkapQueryTemplate instance = new StreamkapQueryTemplate();
        instance.setSFWarehouse(parsedConfig.get(Utils.SF_WAREHOUSE));
        instance.setTargetLag(Integer.parseInt(getOrDefault(parsedConfig.get(Utils.TARGET_LAG_CONF),"15")));
        instance.setCleanupTaskSchedule(Integer.parseInt(getOrDefault(parsedConfig.get(Utils.CLEANUP_TASK_SCHEDULE_CONF),"60")));
        instance.setCreateSqlTemplate(parsedConfig.get(Utils.CREATE_SQL_EXECUTE_CONF));
        instance.setSqlTableNameTemplate(getOrDefault(parsedConfig.get(Utils.SQL_DT_TABLE_NAME_CONF), "{{table}}_DT"));
        instance.setCreateSqlData(getOrDefault(parsedConfig.get(Utils.CREATE_SQL_DATA_CONF), "{}"));
        instance.setSchemaChangeIntervalMs(Long.parseLong(getOrDefault(parsedConfig.get(Utils.SCHEMA_CHANGE_CHECK_MS),"300000")));
        instance.setApplyDynamicTableScript(Boolean.parseBoolean(getOrDefault(parsedConfig.get(Utils.APPLY_DYNAMIC_TABLE_SCRIPT_CONF),"false")));
        TopicConfigProcess topicConfigProcess = new TopicConfigProcess(topics, topicsMapping);
        instance.allTopicConfigs.clear();
        instance.allTopicConfigs.putAll(topicConfigProcess.getAllTopicConfigs());
        instance.schemaCheckTime =System.currentTimeMillis();

        return instance;
    }

    public static String getOrDefault(String value, String defaultValue) {
        return (value == null ? defaultValue : value);
    }

    /**
     * Retrieves the create template for a given topic.
     *
     * @param topic the topic name
     * @return the Mustache create template
     */
    public Mustache getCreateTemplate(String topic) {
        TopicConfig topicConfig = allTopicConfigs.get(topic);
        return topicConfig != null && topicConfig.getCreateTemplate() != null ? topicConfig.getCreateTemplate() : createSqlTemplate;
    }

    
    public Mustache getTableNameTemplate() {
        // table name template is global, same for all topics
        return tableNameTemplate;
    }

    public Map<String, Object> getCreateSqlData() {
        return createSqlData;
    }

    /**
     * Checks if a given topic has a create template.
     *
     * @param topic the topic name
     * @return true if the topic has a create template, false otherwise
     */
    public boolean topicHasCreateTemplate(String topic) {
        TopicConfig topicConfig = allTopicConfigs.get(topic);
        return (topicConfig != null && topicConfig.getCreateTemplate() != null ? true : createSqlTemplate !=null);
    }

    public boolean checkIfDynamicTableExists(String tableName, SnowflakeConnectionService conn) {
        boolean tableExists = false;
        try {
            Connection con = conn.getConnection();
            try (Statement stmt = con.createStatement()) {
                String statement = "DESC DYNAMIC TABLE "+ tableName + "_DT";
                stmt.executeQuery(statement);
                tableExists = true;
            } catch (Exception e) {
                LOGGER.warn("Dynamic table for table {}, doesn't exist.", tableName, e);
            }
        } catch (Exception e) {
            LOGGER.error("Error getting connection", e);
            throw e;
        }

        return tableExists;
    }

    private long getDurationFromStartMs(long startTime) {
        final long currTime = System.currentTimeMillis();
        return currTime - startTime;
    }

    public void checkSchemaChanges(final Collection<SinkRecord> records, Map<String, String> topic2table, SnowflakeConnectionService conn) {

        if (applyDynamicTableScript) {
            for (SinkRecord record : records) {
                if(!processedTopics.containsKey(record.topic())) {
                    recordByTopic.putIfAbsent(record.topic(), record);
                }
            }

            if (getSchemaChangeIntervalMs() > 0
                    && getDurationFromStartMs(schemaCheckTime) >= getSchemaChangeIntervalMs()) {

                for (Map.Entry<String, SinkRecord> entry : recordByTopic.entrySet()) {
                    String topicName = entry.getKey();
                    try {
                        String tableName = Utils.generateValidName(topicName, topic2table);
                        if (!checkIfDynamicTableExists(tableName, conn)) {
                            if (applyCreateScriptIfAvailable(tableName, entry.getValue(), conn)) {
                                recordByTopic.remove(topicName);
                                processedTopics.putIfAbsent(topicName, true);
                            }
                        } else {
                            processedTopics.putIfAbsent(topicName, true);
                        }
                    } catch (Exception e) {
                        //ignore topic due to error
                        LOGGER.info("Error inside PUT function in Streamkap template process");
                    }
                }
                schemaCheckTime = System.currentTimeMillis();
            }
        }
    }

    /**
     * Applies create script if available for the given table name and record.
     *
     * @param tableName the table name
     * @param record    the SinkRecord
     * @param conn      the SnowflakeConnectionService
     */
    public boolean applyCreateScriptIfAvailable(String tableName, SinkRecord record, SnowflakeConnectionService conn) {
        boolean scriptAppliedSuccessfully = false;
        tableName = tableName.replaceAll("\"","");
        if (topicHasCreateTemplate(record.topic())
                && isSFWarehouseExists() ) {
            LOGGER.info("Apply SQL template on table: {}", tableName);
            try {
                Connection con = conn.getConnection();
                try (Statement stmt = con.createStatement()) {
                    Mustache tableNameTemplate = getTableNameTemplate();
                    Map<String, Object> data = getCreateSqlData();
                    String dtTableName = generateSqlFromTemplate(tableName, record, tableNameTemplate, data).get(0);
                    Map<String, Object> dataForTable = new ConcurrentHashMap<>(data);
                    dataForTable.put("dynamicTableName", dtTableName);
                    Mustache template = getCreateTemplate(record.topic());
                    List<String> statements = generateSqlFromTemplate(tableName, record, template, dataForTable);
                    applyDdlStatements(con, statements);
                    processedTopics.putIfAbsent(record.topic(), true);
                    scriptAppliedSuccessfully = true;
                    LOGGER.info("Additional query executed successfully for table: {}", tableName);
                } catch (Exception e) {
                    LOGGER.warn("Failure executing additional statements for table {}.", tableName, e);
                }
            } catch (Exception e) {
                LOGGER.error("Error getting connection", e);
            }
        } else {
            scriptAppliedSuccessfully = true;
            LOGGER.info("Skipping SQL template due to configuration. templateConfig:{} , whExists:{}", topicHasCreateTemplate(record.topic()), isSFWarehouseExists());
        }

        return scriptAppliedSuccessfully;
    }

    private String executeTemplate(String tableName, SinkRecord sinkRecord, Mustache mustacheTemplate, Map<String, Object> data) throws IOException {
        StringWriter writer = new StringWriter();
        Map<String, Object> fieldValues = getRecordDataAsMap(tableName.toUpperCase(), sinkRecord, data);
        mustacheTemplate.execute(writer, fieldValues).flush();
        return writer.toString();
    }

    /**
     * Generates SQL statements from a Mustache template.
     *
     * @param tableName        the table name
     * @param sinkRecord       the SinkRecord
     * @param mustacheTemplate the Mustache template
     * @return a list of SQL statements
     * @throws IOException if an I/O error occurs
     */
    private List<String> generateSqlFromTemplate(String tableName, SinkRecord sinkRecord, Mustache mustacheTemplate, Map<String, Object> data) throws IOException {
        String sqlQueriesStr = executeTemplate(tableName, sinkRecord, mustacheTemplate, data);
        String[] sqlStatements = sqlQueriesStr.split(";"); // Split by semicolon to handle multiple statements
        return Arrays.asList(sqlStatements);
    }

    /**
     * Retrieves record data as a map.
     *
     * @param tableName  the table name
     * @param sinkRecord the SinkRecord
     * @return a map of record data
     */
    private Map<String, Object> getRecordDataAsMap(String tableName, SinkRecord sinkRecord, Map<String, Object> data) {
        Map<String, Object> values = new ConcurrentHashMap<>(data);
        List<String> keyCols = (sinkRecord.keySchema() != null ? sinkRecord.keySchema().fields().stream().map(f -> f.name()) :
                                sinkRecord.valueSchema().fields().stream().map(f -> f.name())).collect(Collectors.toList());
        values.put("warehouse", this.sfWarehouse);
        values.put("targetLag", this.targetLag);
        values.put("schedule", this.cleanupTaskSchedule);
        values.put("table", tableName);
        values.put("primaryKeyColumns", String.join(",", keyCols));
        values.put("keyColumnsAndCondition", String.join("AND", keyCols.stream().map(v-> Utils.quoteNameIfNeeded(tableName) +"."+v+" = subquery."+v).collect(Collectors.toList())));
        // Add more fields as necessary from the sinkRecord
        return values;
    }

    /**
     * Applies DDL statements to the database.
     *
     * @param connection the database connection
     * @param statements the list of DDL statements
     * @throws SQLException if a database access error occurs
     */
    private void applyDdlStatements(Connection connection, List<String> statements) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String ddlStatement : statements) {
                statement.executeUpdate(ddlStatement);
            }
        }
    }

    private void setCreateSqlTemplate(String createSqlExecute) {
        Mustache template = null;
        LOGGER.info("Setting Create SQL Template query : {}", createSqlExecute);
        if (createSqlExecute != null && !createSqlExecute.trim().isEmpty()) {
            template = mustacheFactory.compile(new StringReader(createSqlExecute),  "create-sql-template");
            LOGGER.info("Create SQL Template generated.");
        }
        this.createSqlTemplate = template;
    }

    @SuppressWarnings("unchecked")
    private void setCreateSqlData(String createSqlData) {
        Map<String, Object> result;
        try {
            result = new ObjectMapper().readValue(createSqlData, HashMap.class);
            this.createSqlData = Collections.unmodifiableMap(result);
        } catch (Exception e) {
            throw new RuntimeException("Invalid create sql data " + createSqlData, e);
        }
        LOGGER.info("Setting Create SQL Data : {}", createSqlData);
    }

    

    private void setSqlTableNameTemplate(String sqlTableName) {
        Mustache template = null;
        LOGGER.info("Setting SQL Table Name Template query : {}", sqlTableName);
        if (sqlTableName != null && !sqlTableName.trim().isEmpty()) {
            template = mustacheFactory.compile(new StringReader(sqlTableName),  "sql-tale-name-template");
            LOGGER.info("Create SQL Table Name Template generated.");
        }
        this.tableNameTemplate = template;
    }

    private static class TopicConfig {
        private final Mustache createTemplate;

        public TopicConfig(Mustache createTemplate) {
            this.createTemplate = createTemplate;
        }

        public Mustache getCreateTemplate() {
            return createTemplate;
        }
    }

    private static class TopicConfigProcess {
        private String topicsString;
        private String topicsMapping;
        public static final String TOPICS_CREATE_SQL_EXECUTE = "create.sql.execute";
        private Map<String, TopicConfig> topicConfigs;

        TopicConfigProcess(String topicsString, String topicsMapping) {
            this.topicsString = topicsString;
            this.topicsMapping = topicsMapping;
            this.topicConfigs = parseTopicConfigurations(this.topicsMapping);
        }

        Map<String, TopicConfig> getAllTopicConfigs() {
            return getAllTopicConfig(this.topicConfigs);
        }
        private TopicConfig getTopicConfig(String topic, Map<String, TopicConfig> topicConfig) {
            // Check if the exact topic name is configured, and return the corresponding value
            if (topicConfig.containsKey(topic)) {
                return topicConfig.get(topic);
            }
            // Check if any regex pattern matches the topic name, and return the corresponding value
            for (Map.Entry<String, TopicConfig> entry : topicConfig.entrySet()) {
                if (topic.matches(entry.getKey())) {
                    return entry.getValue();
                }
            }

            return null;
        }

        private Map<String, TopicConfig> getAllTopicConfig(Map<String, TopicConfig> topicConfigs) {
            Map<String, TopicConfig> allTopicConfigs = new ConcurrentHashMap<>();
            if (this.topicsString != null && !this.topicsString.trim().isEmpty()) {
                Arrays.stream(this.topicsString.split(","))
                        .forEach(topic -> {
                            TopicConfig config = getTopicConfig(topic, topicConfigs);
                            if (config == null) {
                                config = new TopicConfig(null);
                            }
                            allTopicConfigs.put(topic, config);
                        });
            }
            return allTopicConfigs;
        }

        private Map<String, TopicConfig> parseTopicConfigurations(String configValue) {
            Map<String, TopicConfig> topicConfigs = new ConcurrentHashMap<>();
            try {
                if (configValue == null || configValue.trim().isEmpty()) {
                    return topicConfigs;
                }

                ObjectMapper mapper = new ObjectMapper();
                Map<String, String> topicConfigMap = mapper.readValue(configValue, Map.class);

                for (Map.Entry<String, String> entry : topicConfigMap.entrySet()) {
                    String topicName = entry.getKey();
                    JsonNode topicConfig = mapper.readTree(entry.getValue());

                    // Fetch topic configuration if present
                    topicConfigs.put(topicName,
                            new TopicConfig(
                                    getTemplateConfig(topicName, TOPICS_CREATE_SQL_EXECUTE, topicConfig)));
                }
            } catch (Exception e) {
                throw new ConfigException("Invalid topic configuration map", e);
            }
            return topicConfigs;
        }

        private Mustache getTemplateConfig(String topicName, String configName, JsonNode topicConfig) {
            Mustache template = null;
            String templateSQL = topicConfig.has(configName) ? topicConfig.get(configName).asText() : null;
            if (templateSQL != null && !templateSQL.trim().isEmpty()) {
                template = mustacheFactory.compile(new StringReader(templateSQL), topicName + "-" + configName);
            }
            return template;
        }
    }
}
