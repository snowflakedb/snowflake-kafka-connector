package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeDataSourceFactory;
import java.lang.reflect.Field;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

public class InjectQueryRunnerExtension implements BeforeEachCallback, ParameterResolver {

  @Override
  public void beforeEach(final ExtensionContext context) throws Exception {
    final Object testInstance = context.getRequiredTestInstance();
    injectFields(testInstance, testInstance.getClass());
  }

  private void injectFields(final Object testInstance, Class<?> testClass) throws Exception {
    // Process fields in the current class
    for (final Field field : testClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(InjectQueryRunner.class)) {
        field.setAccessible(true);
        field.set(testInstance, getQueryRunner());
      }
    }

    // If this is a nested class, recursively process the enclosing class fields
    Class<?> enclosingClass = testClass.getEnclosingClass();
    if (enclosingClass != null) {
      // Get the enclosing instance for nested classes
      Field thisField = getEnclosingInstanceField(testClass);
      if (thisField != null) {
        thisField.setAccessible(true);
        Object enclosingInstance = thisField.get(testInstance);
        if (enclosingInstance != null) {
          injectFields(enclosingInstance, enclosingClass);
        }
      }
    }
  }

  private Field getEnclosingInstanceField(final Class<?> innerClass) {
    try {
      // Non-static inner classes have a synthetic field named "this$0" pointing to the enclosing
      // instance
      for (final Field field : innerClass.getDeclaredFields()) {
        if (field.isSynthetic() && field.getName().startsWith("this$")) {
          return field;
        }
      }
    } catch (final Exception e) {
      // If we can't find the field, return null
    }
    return null;
  }

  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext) {
    return parameterContext.getParameter().isAnnotationPresent(InjectQueryRunner.class)
        && parameterContext.getParameter().getType().equals(QueryRunner.class);
  }

  @Override
  public Object resolveParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext) {
    return getQueryRunner();
  }

  private QueryRunner getQueryRunner() {
    return new QueryRunner(SnowflakeDataSourceFactory.get());
  }
}
