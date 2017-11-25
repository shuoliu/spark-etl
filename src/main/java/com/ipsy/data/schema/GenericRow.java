package com.ipsy.data.schema;

import org.json.JSONObject;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class GenericRow implements Serializable {
  private List<GenericColumn> columns;
  private Map<String, GenericColumn> columnNameLookup;
  private Map<Integer, GenericColumn> columnOrdinalLookup;

  public GenericRow() {
    this.setColumns(null);
  }

  public GenericRow(List<GenericColumn> columns) {
    setColumns(columns);
  }

  public int getColumnCount() {
    return columns.size();
  }

  public List<GenericColumn> getColumns() {
    return new ArrayList<>(columns);
  }

  public void setColumns(List<GenericColumn> columns) {
    this.columns = new ArrayList<>();
    columnNameLookup = new HashMap<>();
    columnOrdinalLookup = new HashMap<>();
    if (columns != null) {
      for (GenericColumn column : columns) {
        addColumn(column);
      }
    }
  }

  public GenericColumn getAt(int i) {
    return columnOrdinalLookup.get(i);
  }

  public GenericColumn getColumn(String name) {
    return columnNameLookup.get(name);
  }

  public boolean addColumn(GenericColumn column) {
    if (column == null || column.getDefinition().getColumnName() == null
        || columnNameLookup.containsKey(column.getDefinition().getColumnName())
        || columnOrdinalLookup.containsKey(column.getDefinition().getOrdinalPosition())) {
      return false;
    }
    columns.add(column);
    columnNameLookup.put(column.getDefinition().getColumnName(), column);
    columnOrdinalLookup.put(column.getDefinition().getOrdinalPosition(), column);
    return true;
  }

  public GenericColumn dropColumn(String name) {
    if (name == null || !columnNameLookup.containsKey(name)) {
      return null;
    }
    GenericColumn column = columnNameLookup.get(name);
    columnNameLookup.remove(name);
    columnOrdinalLookup.remove(column.getDefinition().getOrdinalPosition());
    columns.remove(column);
    return column;
  }

  public GenericRow clone() {
    GenericRow newRow = new GenericRow();
    List<GenericColumn> newColumns = new ArrayList<>();
    for (GenericColumn column : this.columns) {
      newColumns.add(column.clone());
    }
    newRow.setColumns(new ArrayList<>(newColumns));
    return newRow;
  }

  public String toJsonString() {
    JSONObject json = new JSONObject();
    for (GenericColumn column : getColumns()) {
      ColumnDefinition definition = column.getDefinition();
      Object value = column.getValue();
      if (definition.getDataType().equals("bigint")) {
        value = column.parseLong();
      } else if (definition.getDataType().equals("boolean")) {
        value = column.parseBoolean();
      }
      json.put(definition.getColumnName(), value);
    }
    return json.toString();
  }

  public String toString() {
    return toString(",");
  }

  public String toString(String delimiter) {
    columns.sort(Comparator.comparingInt(o -> o.getDefinition().getOrdinalPosition()));
    List<String> fields = columns.stream().map(GenericColumn::getValue).collect(Collectors.toList());
    return String.join(delimiter, fields);
  }

}
