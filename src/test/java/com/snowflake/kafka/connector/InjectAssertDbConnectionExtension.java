package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeDataSourceFactory;
import java.lang.reflect.Field;
import org.assertj.db.type.AssertDbConnection;
import org.assertj.db.type.AssertDbConnectionFactory;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

public final class InjectAssertDbConnectionExtension
    implements BeforeEachCallback, ParameterResolver {

  @Override
  public void beforeEach(final ExtensionContext context) throws Exception {
    final Object testInstance = context.getRequiredTestInstance();
    final Class<?> testClass = testInstance.getClass();

    // Inject AssertDbConnection into fields annotated with @InjectAssertDbConnection
    for (final Field field : testClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(InjectAssertDbConnection.class)) {
        field.setAccessible(true);
        field.set(testInstance, getAssertDbConnection());
      }
    }
  }

  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext) {
    return parameterContext.getParameter().isAnnotationPresent(InjectAssertDbConnection.class)
        && parameterContext.getParameter().getType().equals(AssertDbConnection.class);
  }

  @Override
  public Object resolveParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext) {
    return getAssertDbConnection();
  }

  private AssertDbConnection getAssertDbConnection() {
    return AssertDbConnectionFactory.of(SnowflakeDataSourceFactory.get()).create();
  }
}
