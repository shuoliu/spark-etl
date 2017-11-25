package com.ipsy.data.schema;

import java.util.ArrayList;
import java.util.List;

public class SchemaDefinition {
  private String dbIdentifier;
  private String tableName;
  private List<ColumnDefinition> columnDefinitions;

  public SchemaDefinition() {
    columnDefinitions = new ArrayList<>();
  }

  public String getDbIdentifier() {
    return dbIdentifier;
  }

  public void setDbIdentifier(String dbIdentifier) {
    this.dbIdentifier = dbIdentifier;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }
}
