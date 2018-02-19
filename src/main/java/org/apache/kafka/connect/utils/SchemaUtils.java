package org.apache.kafka.connect.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrea Patelli
 */
public class SchemaUtils {
	private final static Logger log = LoggerFactory.getLogger(SchemaUtils.class);
	
    public static Map<String, Object> toJsonMap(Struct struct) {
        Map<String, Object> jsonMap = new HashMap<String, Object>(0);
        List<Field> fields = struct.schema().fields();
        for (Field field : fields) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            if (log!=null && struct!=null && struct.getWithoutDefault(fieldName)!=null)
            {
            	log.trace("Field type for field "+fieldName+" : "+struct.getWithoutDefault(fieldName).getClass().getName());
            }
            switch (fieldType) {
                case STRING:
                    jsonMap.put(fieldName, struct.getString(fieldName));
                    break;
                case INT32:
                    jsonMap.put(fieldName, struct.getInt32(fieldName));
                    break;
                case INT16:
                    jsonMap.put(fieldName, struct.getInt16(fieldName));
                    break;
                case INT64:
                	if (struct == null || struct.getWithoutDefault(fieldName)==null || (!struct.getWithoutDefault(fieldName).getClass().getName().equals("java.util.Date")))
                	{
                		jsonMap.put(fieldName, struct.getInt64(fieldName));
                	}
                	else
                	{
                		jsonMap.put(fieldName, (java.util.Date)struct.getWithoutDefault(fieldName));
                	}
                    break;
                case INT8:
                	jsonMap.put(fieldName, struct.getInt8(fieldName));
                	break;
                case FLOAT32:
                    jsonMap.put(fieldName, struct.getFloat32(fieldName));
                    break;
                case STRUCT:
                    jsonMap.put(fieldName, toJsonMap(struct.getStruct(fieldName)));
                    break;
                case FLOAT64:
                	jsonMap.put(fieldName, struct.getFloat64(fieldName));
                    break;
                case BYTES:
                	log.error("toto : "+struct.getWithoutDefault(fieldName).getClass().getName());
                	if (struct == null || struct.getWithoutDefault(fieldName)==null || (!struct.getWithoutDefault(fieldName).getClass().getName().equals("java.util.int8")))
                	{
                		jsonMap.put(fieldName, struct.getBytes(fieldName));
                	}
                	else
                	{
                		jsonMap.put(fieldName, struct.getBoolean(fieldName));
                	}
                	break;
                case ARRAY:
                	jsonMap.put(fieldName, struct.getArray(fieldName));
                	break;
                case MAP:
                	jsonMap.put(fieldName, struct.getMap(fieldName));
                	break;
                default:
                	log.error("Unknown field type : "+fieldType.getName());
            }
        }
        return jsonMap;
    }
}
