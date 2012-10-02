package com.linkedin.gazelle;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

import com.linkedin.gazelle.utils.GazelleColumnMedata;
import com.linkedin.gazelle.utils.GazelleColumnType;

public class TestHelper {
  
  public static GazelleColumnMedata[] setUpColumnMetadataArr(Schema schema) {
    GazelleColumnMedata[] columnMetadataArr = new GazelleColumnMedata[schema.getFields().size()];
    List<Field> fields = schema.getFields();
    columnMetadataArr = new GazelleColumnMedata[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      columnMetadataArr[i] = new GazelleColumnMedata();
      columnMetadataArr[i].setName(fields.get(i).name());
      columnMetadataArr[i].setColumnType(getOriginalColumnTypeFromField(fields.get(i)));
    }
    return columnMetadataArr;
  }

  public static GazelleColumnType getOriginalColumnTypeFromField(Field field) {
    Type type = field.schema().getType();
    if (type == Type.UNION) {
      type = ((Schema) CollectionUtils.find(field.schema().getTypes(), new Predicate() {
        @Override
        public boolean evaluate(Object object) {
          return ((Schema) object).getType() != Type.NULL;
        }
      })).getType();
    }
    return GazelleColumnType.getType(type.toString());
  }
}
