package net.radai.avropartial;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.HdrHistogram.IntCountsHistogram;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Assert;
import org.junit.Test;


public class PartialSchemaBenchmark {

  private String schemaFileName = "super.avsc";
  private int numObjects = 100000;
  private int stringSizeLimit = 256;
  private int bytesSizeLimit = 64 * 1024; //64K
  private String field = "header";
  private int repetitions = 10;


  @Test
  public void benchmark() throws Exception {

    Schema superSchema;
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(schemaFileName)) {
      Schema.Parser parser = new Schema.Parser();
      parser.setValidate(true);
      parser.setValidateDefaults(true);
      superSchema = parser.parse(is);
    }

    Schema subSchema = PartialSchemaBuilder.buildPartial(superSchema, field);

    Random r = new Random(System.currentTimeMillis());
    GenericDatumWriter<GenericData.Record> writer = new ReflectDatumWriter<>(superSchema);
    GenericDatumWriter<GenericData.Record> subWriter = new ReflectDatumWriter<>(subSchema);
    GenericDatumReader<GenericData.Record> reader = new ReflectDatumReader<>(superSchema);
    GenericDatumReader<GenericData.Record> subReader = new ReflectDatumReader<>(subSchema);
    IntCountsHistogram originalSizes = new IntCountsHistogram(3);
    IntCountsHistogram subSizes = new IntCountsHistogram(3);

    GenericData.Record[] originalObjects = new GenericData.Record[numObjects];
    byte[][] serializedObjects = new byte[originalObjects.length][];

    for (int i=0; i<originalObjects.length; i++) {
      GenericData.Record record = (GenericData.Record) random(superSchema, r);
      byte[] serialized = serialize(record, writer);
      byte[] subSerialized = serialize(record, subWriter);

      originalSizes.recordValue(serialized.length);
      subSizes.recordValue(subSerialized.length);
      originalObjects[i] = record;
      serializedObjects[i] = serialized;

      GenericData.Record subRecord = deserialize(serialized, subReader, null);
      Assert.assertNotNull(subRecord);
      Assert.assertEquals(record.get(field), subRecord.get(field));

      if (i % 10000 == 0) {
        System.err.println("generated " + i + "/" + originalObjects.length);
      }
    }

    System.err.println("original sizes:");
    originalSizes.outputPercentileDistribution(System.err, 1.0);
    System.err.println("subSchema sizes:");
    subSizes.outputPercentileDistribution(System.err, 1.0);
    System.err.println("size ratio: " + subSizes.getMean() / originalSizes.getMean());

    //save some memory
    originalSizes = null;
    subSizes = null;

    List<Long> fullTimings = new ArrayList<>(repetitions);
    for (int i=0; i<repetitions; i++) {
      fullTimings.add(timeDeserialize(serializedObjects, reader));
    }
    Collections.sort(fullTimings);

    List<Long> subTimings = new ArrayList<>(repetitions);
    for (int i=0; i<repetitions; i++) {
      subTimings.add(timeDeserialize(serializedObjects, subReader));
    }
    Collections.sort(subTimings);

    int h = 8;
  }

  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

  private static byte[] serialize(
      GenericData.Record record,
      GenericDatumWriter<GenericData.Record> writer) throws Exception {

    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      BinaryEncoder enc = ENCODER_FACTORY.binaryEncoder(os, null);
      writer.write(record, enc);
      enc.flush();
      os.flush();
      return os.toByteArray();
    }
  }

  private static GenericData.Record deserialize(
      byte[] bytes,
      GenericDatumReader<GenericData.Record> reader,
      BinaryDecoder reuse
  ) throws Exception {
    BinaryDecoder dec = DECODER_FACTORY.binaryDecoder(bytes, reuse);
    return reader.read(null, dec);
  }

  long timeDeserialize(byte[][] serializedObjects, GenericDatumReader<GenericData.Record> reader) throws Exception {
    BinaryDecoder dec = DECODER_FACTORY.binaryDecoder(new byte[]{}, null);
    long start = System.currentTimeMillis();
    for (byte[] bytes : serializedObjects) {
      GenericData.Record deserialized = deserialize(bytes, reader, dec);
      Assert.assertNotNull(deserialized); //we need to do something with the output or it might get optimized away
    }
    return System.currentTimeMillis() - start;
  }

  private static final String ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_ ";

  private Object random(Schema s, Random r) {
    int size;
    switch (s.getType()) {
      case RECORD:
        GenericData.Record instance = new GenericData.Record(s);
        for (Schema.Field f : s.getFields()) {
          Object fieldValue = random(f.schema(), r);
          instance.put(f.pos(), fieldValue);
        }
        return instance;
      case STRING:
        size = r.nextInt(stringSizeLimit + 1);
        StringBuilder sb = new StringBuilder(size);
        for (int i=0; i<size; i++) {
          sb.append(ALPHABET.charAt(r.nextInt(ALPHABET.length())));
        }
        return sb.toString();
      case LONG:
        return r.nextLong();
      case BYTES:
        size = r.nextInt(bytesSizeLimit + 1);
        byte[] bytes = new byte[size];
        r.nextBytes(bytes);
        return bytes;
      default:
        throw new IllegalArgumentException("unhandled type " + s.getType());
    }
  }
}
