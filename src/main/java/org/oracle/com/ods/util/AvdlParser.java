package org.oracle.com.ods.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvdlParser {
    private static final Logger logger = LoggerFactory.getLogger(AvdlParser.class);

    private Schema schema;

    /**
     * Parses the Avro IDL file to extract the recordId and Json Schema and List of fields.
     * @param filepath the path to the Avro IDL file
     */
    public AvdlParser(String filepath) throws IOException, ParseException {
        File inputFile = new File(filepath);
        Idl idl = new Idl(inputFile);
        Protocol protocol = idl.CompilationUnit();
        this.schema = getSchema(protocol);
        if (this.schema == null) {
            throw new IllegalArgumentException("No schema found in the Avro IDL file.");
        }
    }

    private Schema getSchema(Protocol protocol) {
        for (Schema s : protocol.getTypes()) {
            return s; // Take the first (or only) schema
        }
        return null;
    }

    public String getJsonSchema() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("type", "record");
        rootNode.put("name", schema.getName());
        rootNode.put("namespace", schema.getNamespace());

        ArrayNode fieldsArray = mapper.createArrayNode();
        for (Schema.Field field : schema.getFields()) {
            ObjectNode fieldNode = mapper.createObjectNode();
            fieldNode.put("name", field.name());

            if (field.schema().getType() == Schema.Type.UNION) {
                ArrayNode unionTypes = mapper.createArrayNode();
                for (Schema unionSchema : field.schema().getTypes()) {
                    if (unionSchema.getType() == Schema.Type.NULL) {
                        unionTypes.add("null");
                    } else {
                        unionTypes.add(unionSchema.getType().getName());
                    }
                }
                fieldNode.set("type", unionTypes);
            } else {
                fieldNode.put("type", field.schema().getType().getName());
            }

            // Handle default value
            if (field.hasDefaultValue()) {
                Object defaultVal = field.defaultVal();
                if (defaultVal == null || defaultVal instanceof org.apache.avro.JsonProperties.Null) {
                    fieldNode.putNull("default");
                } else {
                    fieldNode.put("default", defaultVal.toString());
                }
            }
            fieldsArray.add(fieldNode);
        }
        rootNode.set("fields", fieldsArray);

        return mapper.writeValueAsString(rootNode);
    }

    /**
     * Parses field names from the Avro IDL file.
     * @return a list of field names
     */
    public List<String> getFields() {
        List<String> fields = new ArrayList<>();
        for (Schema.Field field : schema.getFields()) {
            fields.add(field.name());
        }
        return fields;
    }

    /**
     * Parses namespace and recordName from the Avro IDL file.
     * @return recordId
     */
    public String getRecordId() {
        String namespace = schema.getNamespace();
        String recordName = schema.getName();
        return namespace + "." + recordName;
    }
}

