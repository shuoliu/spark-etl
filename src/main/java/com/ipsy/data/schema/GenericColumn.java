package com.ipsy.data.schema;

import java.io.Serializable;

public class GenericColumn implements Serializable {
  private ColumnDefinition definition;
  private String value;

  public GenericColumn(ColumnDefinition columnDefinition) {
    this.definition = columnDefinition;
    this.value = columnDefinition.getDefaultValue();
  }

  public GenericColumn(ColumnDefinition columnDefinition, String columnValue) {
    this.definition = columnDefinition;
    this.value = columnValue;
  }

  public Boolean parseBoolean() {
    return "t".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value);
  }

  public Long parseLong() {
    return Long.parseLong(value);
  }

  public Integer parseInt() {
    return Integer.parseInt(value);
  }

  public ColumnDefinition getDefinition() {
    return definition;
  }

  public String getValue() {
    return value;
  }

  public GenericColumn clone() {
    return new GenericColumn(this.definition, this.value);
  }
}
