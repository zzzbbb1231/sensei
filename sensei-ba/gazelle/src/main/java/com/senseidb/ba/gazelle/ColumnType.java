package com.senseidb.ba.gazelle;

import org.apache.avro.Schema.Type;
import org.springframework.util.Assert;


/**
 * @author dpatel
 */

public enum ColumnType {
  INT, LONG, FLOAT, STRING, INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, STRING_ARRAY;

  public static ColumnType valueOfStr(String name) {
    name = name.toUpperCase();
    if ("DOUBLE".equals(name)) {
      return FLOAT;
    }
    return valueOf(name);
  }
  public static ColumnType valueOf(Class<?> cls) {
    if (cls == int.class || cls == Integer.class || cls == short.class || cls == Short.class) {
      return INT;
    }
    if (cls == long.class || cls == Long.class) {
      return LONG;
    }
    if (cls == String.class) {
      return STRING;
    }
    if (cls == Double.class || cls == Float.class || cls == float.class || cls == double.class) {
      return FLOAT;
    }
    throw new UnsupportedOperationException(cls.toString());
  }
  public static ColumnType valueOf(Type type) {
      if (type == Type.INT) {
      return INT;
    }
    if (type == Type.LONG) {
      return LONG;
    }
    if (type == Type.STRING) {
      return STRING;
    }
    if (type == Type.DOUBLE || type == Type.FLOAT) {
      return FLOAT;
    }
    throw new UnsupportedOperationException(type.toString());
  }
  public static ColumnType valueOfArrayType(Type type) {
      if (type == Type.INT) {
      return INT_ARRAY;
    }
    if (type == Type.LONG) {
      return LONG_ARRAY;
    }
    if (type == Type.STRING) {
      return STRING_ARRAY;
    }
    if (type == Type.DOUBLE || type == Type.FLOAT) {
      return FLOAT_ARRAY;
    }
    throw new UnsupportedOperationException(type.toString());
  }
  public static ColumnType valueOfArrayType(ColumnType type) {
      if (type == ColumnType.INT) {
      return INT_ARRAY;
    }
    if (type == ColumnType.LONG) {
      return LONG_ARRAY;
    }
    if (type == ColumnType.STRING) {
      return STRING_ARRAY;
    }
    if (type == ColumnType.FLOAT) {
      return FLOAT_ARRAY;
    }
    throw new UnsupportedOperationException(type.toString());
  }
  public boolean isMulti() {
      return name().endsWith("_ARRAY");
  }
  public ColumnType getElementType() {
     if (isMulti()) {
       return valueOfStr(name().substring(0, name().indexOf("_ARRAY")));
     } else {
       return this;
     }
  }
  public static ColumnType getColumnType(Object obj) {
      if (obj == null) {
          return null;
      }
      if (obj instanceof Object[]) {
          Object[] arr = (Object[]) obj;
          if (arr.length == 0) {
              return null;
          }
          ColumnType currentType = null;
          for (Object element : arr) {
              ColumnType newType = getColumnType(element);
              if (isBigger(currentType, newType)) {
                  currentType = newType;
              }
          }
          return ColumnType.valueOfArrayType(currentType);
      } else {
          if (obj instanceof Number) {
              Number number = (Number) obj;
              if (obj instanceof Integer || obj instanceof Short || obj instanceof Byte) {
                  return ColumnType.INT;
              }
              if (obj instanceof Long) {
                  return (number.longValue() > Integer.MAX_VALUE ? ColumnType.LONG : ColumnType.INT);
              } else {
                  return ColumnType.FLOAT;
              }
          } else if (obj instanceof String){
              return ColumnType.STRING;
          }
      }
      throw new UnsupportedOperationException(obj.getClass().toString());
  }
  public static boolean isBigger(ColumnType currentType, ColumnType newType) {
      if (newType == null) {
          return false;
      }
      if (currentType == null) return true;
      return newType.ordinal() - currentType.ordinal() > 0;
  }
}
