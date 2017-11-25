package com.ipsy.data.schema;

import org.junit.Assert;
import org.junit.Test;

public class GenericRowTest {
  private static final ColumnDefinition booleanColumnDefinition = new ColumnDefinition("boolean_column", null, "boolean", "t", true, 0, "test");
  private static final ColumnDefinition bigintColumnDefinition = new ColumnDefinition("bigint_column", null, "bigint", "111", true, 1, "test");
  private static final ColumnDefinition varcharColumnDefinition = new ColumnDefinition("varchar_column", 255, "character varying", "abc", true, 2, "test");
  private static final ColumnDefinition nullColumnDefinition = new ColumnDefinition("varchar_column", null, "character varying", null, true, 2, "test");

  @Test
  public void testToJsonString() {
    GenericRow row = new GenericRow();
    row.addColumn(new GenericColumn(booleanColumnDefinition, "t"));
    row.addColumn(new GenericColumn(bigintColumnDefinition, "123"));
    row.addColumn(new GenericColumn(varcharColumnDefinition, "aaa"));
    row.addColumn(new GenericColumn(nullColumnDefinition));
    String actual = row.toJsonString();
    String expected = "{\"bigint_column\":123,\"boolean_column\":true,\"varchar_column\":\"aaa\"}";
    Assert.assertEquals(expected, actual);
  }
}
