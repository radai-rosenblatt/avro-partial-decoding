package net.radai.avropartial;

import java.io.InputStream;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;


public class PartialSchemaBuilderTest {

  @Test
  public void testSchemaStripping() throws Exception {
    Schema superSchema;
    try (InputStream is = getClass().getClassLoader().getResourceAsStream("super.avsc")) {
      Schema.Parser parser = new Schema.Parser();
      parser.setValidate(true);
      parser.setValidateDefaults(true);
      superSchema = parser.parse(is);
    }
    Schema partialSchema = PartialSchemaBuilder.buildPartial(superSchema, "header");
    Assert.assertNotNull(partialSchema);
    Assert.assertNotNull(partialSchema.getField("f0"));
    Assert.assertNotNull(partialSchema.getField("header"));
    Assert.assertNull(partialSchema.getField("f1"));
  }
}
