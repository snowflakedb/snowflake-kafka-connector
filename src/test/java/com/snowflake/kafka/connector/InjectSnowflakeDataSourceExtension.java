package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeDataSourceFactory;
import java.lang.reflect.Field;
import javax.sql.DataSource;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

@Order(1)
public class InjectSnowflakeDataSourceExtension implements BeforeEachCallback, ParameterResolver {

  @Override
  public void beforeEach(final ExtensionContext context) throws Exception {
    final Object testInstance = context.getRequiredTestInstance();
    final Class<?> testClass = testInstance.getClass();

    // Inject datasource into fields annotated with @InjectSnowflakeDataSource
    for (final Field field : testClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(InjectSnowflakeDataSource.class)) {
        field.setAccessible(true);
        field.set(testInstance, SnowflakeDataSourceFactory.get());
      }
    }
  }

  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext) {
    return parameterContext.getParameter().isAnnotationPresent(InjectSnowflakeDataSource.class)
        && parameterContext.getParameter().getType().equals(DataSource.class);
  }

  @Override
  public Object resolveParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext) {
    return SnowflakeDataSourceFactory.get();
  }
}
