package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.Field;

public class InjectQueryRunnerExtension implements BeforeEachCallback, ParameterResolver {

    @Override
    public void beforeEach(final ExtensionContext context) throws Exception {
        final Object testInstance = context.getRequiredTestInstance();
        final Class<?> testClass = testInstance.getClass();

        // Inject QueryRunner into fields annotated with @InjectQueryRunner
        for (final Field field : testClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(InjectQueryRunner.class)) {
                field.setAccessible(true);
                field.set(testInstance, getQueryRunner());
            }
        }
    }

    @Override
    public boolean supportsParameter(
            final ParameterContext parameterContext,
            final ExtensionContext extensionContext) {
        return parameterContext.getParameter().isAnnotationPresent(InjectQueryRunner.class)
                && parameterContext.getParameter().getType().equals(QueryRunner.class);
    }

    @Override
    public Object resolveParameter(
            final ParameterContext parameterContext,
            final ExtensionContext extensionContext) {
        return getQueryRunner();
    }

    private QueryRunner getQueryRunner() {
        return new QueryRunner(SnowflakeDataSourceFactory.get());
    }
}

