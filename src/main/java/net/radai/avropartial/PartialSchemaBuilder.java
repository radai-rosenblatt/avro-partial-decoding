package net.radai.avropartial;

import java.io.StringReader;
import java.util.Map;
import java.util.Set;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.apache.avro.Schema;


public class PartialSchemaBuilder {
  public static Schema buildPartial(Schema superSchema, String fieldUpTo) {
    String schemaJson = superSchema.toString(true);
    JsonObject jsonObject = Json.createReader(new StringReader(schemaJson)).readObject();
    JsonObjectBuilder partialObjectBuilder = Json.createObjectBuilder();
    Set<Map.Entry<String, JsonValue>> properties = jsonObject.entrySet();
    for (String propName : jsonObject.keySet()) {
      if (propName.equals("fields")) {
        JsonArrayBuilder partialFieldsBuilder = Json.createArrayBuilder();
        JsonArray fields = jsonObject.getJsonArray(propName);
        int fieldIndex = -1;
        for (int i=0; i<fields.size(); i++) {
          JsonObject field = fields.getJsonObject(i);
          JsonObjectBuilder fieldObjectBuilder = Json.createObjectBuilder();
          String fieldName = field.getString("name");
          for (String fieldPropName : field.keySet()) {
            fieldObjectBuilder.add(fieldPropName, field.get(fieldPropName));
          }
          partialFieldsBuilder.add(fieldObjectBuilder);
          if (fieldName.equals(fieldUpTo)) {
            fieldIndex = i;
            break;
          }
        }
        if (fieldIndex == -1) {
          throw new IllegalStateException("field " + fieldUpTo + " not found in input schema");
        }
        partialObjectBuilder.add(propName, partialFieldsBuilder);
      } else {
        //copy all other properties as-is (type, name, docs, whatever)
        partialObjectBuilder.add(propName, jsonObject.get(propName));
      }
    }
    String partialSchemaJson = partialObjectBuilder.build().toString();
    Schema.Parser parser = new Schema.Parser();
    parser.setValidate(true);
    parser.setValidateDefaults(true);
    return parser.parse(partialSchemaJson);
  }
}
