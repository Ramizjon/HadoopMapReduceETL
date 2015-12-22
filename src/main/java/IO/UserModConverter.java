package IO;

import dataTO.UserModCommand;
import parquet.example.data.Group;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

class UserModConverter extends GroupConverter {
  private final UserModConverter parent;
  private final int index;
  protected UserModCommand current;
  private Converter[] converters;

  UserModConverter(UserModConverter parent, int index, GroupType schema) {
    this.parent = parent;
    this.index = index;

    converters = new Converter[schema.getFieldCount()];

    for (int i = 0; i < converters.length; i++) {
      final Type type = schema.getType(i);
      if (type.isPrimitive()) {
        converters[i] = new UserModPrimitiveConverter(this, i);
      } else {
        converters[i] = new UserModConverter(this, i, type.asGroupType());
      }

    }
  }

  @Override
  public void start() {
    current = parent.getCurrentRecord();
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void end() {
  }

  public UserModCommand getCurrentRecord() {
    return current;
  }
}