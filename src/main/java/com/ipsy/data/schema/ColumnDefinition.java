package com.ipsy.data.schema;

import java.io.Serializable;

public class ColumnDefinition implements Serializable {
  private String columnName;
  private Integer dataSize;
  private String dataType;
  private String defaultValue;
  private Boolean isNullable;
  private Integer ordinalPosition;
  private String tableName;

  public ColumnDefinition(String columnName, Integer dataSize, String dataType, String defaultValue, Boolean isNullable,
                          Integer ordinalPosition, String tableName) {
    this.columnName = columnName;
    this.dataSize = dataSize;
    this.dataType = dataType;
    this.defaultValue = defaultValue;
    this.isNullable = isNullable;
    this.ordinalPosition = ordinalPosition;
    this.tableName = tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public Integer getDataSize() {
    return dataSize;
  }

  public String getDataType() {
    return dataType;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public Boolean getNullable() {
    return isNullable;
  }

  public Integer getOrdinalPosition() {
    return ordinalPosition;
  }

  public String getTableName() {
    return tableName;
  }

}
