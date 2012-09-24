package com.senseidb.ba.trevni;

public enum DataType {
  STRING("java.lang.String"), LONG("java.lang.Long"), INT("java.lang.Integer");
  
  private String javaClassType;

  DataType(String classType) {
    javaClassType = classType;
  }

  public static DataType getType(String klass) {
    for (DataType type : DataType.values()) {
      if (klass.equals(type.toString())) {
        return type;
      }
    }
    return null;
  }

  public static Class<?> getClassFromEnumType(DataType type) throws ClassNotFoundException {
    return Class.forName(type.javaClassType);
  }

  public static Class<?> getClassFromStringType(String klass) throws ClassNotFoundException {
    for (DataType type : DataType.values()) {
      if (klass.equals(type.toString())) {
        return Class.forName(type.javaClassType);
      }
    }
    return null;
  }
}
