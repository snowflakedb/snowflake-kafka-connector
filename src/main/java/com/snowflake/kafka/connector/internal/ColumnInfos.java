package com.snowflake.kafka.connector.internal;

public class ColumnInfos {
    private final String columnName;
    private final String comments;

    public ColumnInfos(String columnName, String comments) {
        this.columnName = columnName;
        this.comments = comments;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getComments() {
        return comments;
    }
}
