package com.aggregator.utils;

import org.apache.avro.Schema;
import parquet.hadoop.ParquetWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ParquetAppender <T> {
   private Path path;
   private Schema schema;
   private final Class<T> type;
   ParquetWriter<T> writer;

    public ParquetAppender(String path, Class<T> type){
        try {
            this.type = type;
            String resPath = path.concat("/out/result");
            this.path = new Path(resPath);
            initComponents(1, CompressionCodecName.UNCOMPRESSED, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void initComponents(int num, CompressionCodecName compression,
                                         boolean enableDictionary) throws IOException {
         schema = ReflectData.get().getSchema(type);
         writer = AvroParquetWriter.<T>builder(path)
                .withSchema(schema)
                .withCompressionCodec(compression)
                .withDataModel(ReflectData.get())
                .withDictionaryEncoding(enableDictionary)
                .build();
    }

    public void append(T object) throws IOException {
        writer.write(object);
    }

    public void close (){
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
