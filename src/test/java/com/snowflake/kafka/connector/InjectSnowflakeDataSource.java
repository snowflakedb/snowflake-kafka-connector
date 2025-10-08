package com.snowflake.kafka.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
/**
 * Annotation to mark fields and parameters for DataSource injection.
 * Use with {@link InjectSnowflakeDataSourceExtension}.
 */
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface InjectSnowflakeDataSource {
}

