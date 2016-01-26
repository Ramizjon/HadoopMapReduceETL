package IO;

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;


abstract public class UserConverter extends Converter {

  @Override
  public boolean isPrimitive() {
    return false;
  }

  public UserConverter asUserConverter() {
    return this;
  }


  abstract public Converter getConverter(int fieldIndex);

  abstract public void start();
  
  abstract public void end();

}
