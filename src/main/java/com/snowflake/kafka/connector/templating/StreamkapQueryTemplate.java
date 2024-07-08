package com.snowflake.kafka.connector.templating;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class StreamkapQueryTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamkapQueryTemplate.class);
    private static final StreamkapQueryTemplate INSTANCE = new StreamkapQueryTemplate();
    private final ConcurrentMap<String, TopicConfig> allTopicConfigs = new ConcurrentHashMap<>();
    private Mustache createSqlTemplate = null;
    private String sfWarehouse;
    private int targetLag = 15;
    private boolean isSFWarehouseExists = false;
    static MustacheFactory mustacheFactory = new DefaultMustacheFactory();

    private StreamkapQueryTemplate() {
        // Private constructor to prevent instantiation
    }

    public static StreamkapQueryTemplate getInstance() {
        return INSTANCE;
    }

    public void setSFWarehouse (String sfWarehouse) {
        this.sfWarehouse = sfWarehouse;
        this.isSFWarehouseExists = (this.sfWarehouse != null && !this.sfWarehouse.trim().isEmpty());
    }

    public boolean isSFWarehouseExists () {
        return this.isSFWarehouseExists;
    }
    public void setTargetLag(int targetLag) {
        this.targetLag = targetLag;
    }

    /**
     * Initializes the topic configurations based on provided topics and mappings.
     *
     * @param topics        the topics
     * @param topicsMapping the topic mappings
     */
    public static void initConfigDef(final String topics, final String topicsMapping,
                                     final String createSqlExecute, final String sfWarehouse,
                                     final int targetLag) {
        StreamkapQueryTemplate instance = getInstance();
        instance.setSFWarehouse(sfWarehouse);
        instance.setTargetLag(targetLag);
        instance.setCreateSqlTemplate(createSqlExecute);
        TopicConfigProcess topicConfigProcess = new TopicConfigProcess(topics, topicsMapping);
        instance.allTopicConfigs.clear();
        instance.allTopicConfigs.putAll(topicConfigProcess.getAllTopicConfigs());
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

    /**
     * Applies create script if available for the given table name and record.
     *
     * @param tableName the table name
     * @param record    the SinkRecord
     * @param conn      the SnowflakeConnectionService
     */
    public static void applyCreateScriptIfAvailable(String tableName, SinkRecord record, SnowflakeConnectionService conn) {
        StreamkapQueryTemplate instance = getInstance();
        if (instance.topicHasCreateTemplate(record.topic())
                && instance.isSFWarehouseExists() ) {
            LOGGER.info("Apply SQL template ...");
            try {
                Connection con = conn.getConnection();
                try (Statement stmt = con.createStatement()) {
                    Mustache template = instance.getCreateTemplate(record.topic());
                    List<String> statements = instance.generateSqlFromTemplate(tableName, record, template);
                    instance.applyDdlStatements(con, statements);

                    LOGGER.info("Additional query executed successfully for table: {}", tableName);
                } catch (Exception e) {
                    LOGGER.warn("Failure executing additional statements for table {}. This could happen when multiple partitions try to alter the table at the same time and the warning could be ignored.", tableName, e);
                }
            } catch (Exception e) {
                LOGGER.error("Error getting connection", e);
            }
        } else {
            LOGGER.info("Skipping SQL template due to configuration. templateConfig:{} , whExists:{}", instance.topicHasCreateTemplate(record.topic()), instance.isSFWarehouseExists());
        }
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
    private List<String> generateSqlFromTemplate(String tableName, SinkRecord sinkRecord, Mustache mustacheTemplate) throws IOException {
        StringWriter writer = new StringWriter();
        Map<String, Object> fieldValues = getRecordDataAsMap(tableName, sinkRecord);
        mustacheTemplate.execute(writer, fieldValues).flush();
        String[] sqlStatements = writer.toString().split(";"); // Split by semicolon to handle multiple statements
        return Arrays.asList(sqlStatements);
    }

    /**
     * Retrieves record data as a map.
     *
     * @param tableName  the table name
     * @param sinkRecord the SinkRecord
     * @return a map of record data
     */
    private Map<String, Object> getRecordDataAsMap(String tableName, SinkRecord sinkRecord) {
        Map<String, Object> values = new ConcurrentHashMap<>();
        List<String> keyCols = (sinkRecord.keySchema() != null ? sinkRecord.keySchema().fields().stream().map(f -> f.name()) :
                                sinkRecord.valueSchema().fields().stream().map(f -> f.name())).collect(Collectors.toList());
        values.put("warehouse", this.sfWarehouse);
        values.put("targetLag", this.targetLag);
        values.put("table", tableName);
        values.put("primaryKeyColumns", String.join(",", keyCols));
        values.put("keyColumnsAndCondition", String.join("AND", keyCols.stream().map(v-> tableName+"."+v+" = subquery."+v).collect(Collectors.toList())));
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
