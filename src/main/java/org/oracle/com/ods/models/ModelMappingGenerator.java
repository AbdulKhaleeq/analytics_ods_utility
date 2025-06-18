package org.oracle.com.ods.models;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ModelMappingGenerator {
    /**
     * Generates a JSON object representing the model mapping for a given table and metadata.
     *
     * @param recordMap   ObjectNode representing the record map.
     * @param targetModels ArrayNode representing the target models.
     * @param mappingId   Uniquely generated mappingId for model mapping file.
     * @param entityType  Entity of the table from crawler source yaml file.
     * @param schema      Base64 encoded schema for ods model mapping.
     * @return modelMapping ObjectNode representing the model mapping.
     */
    public static ObjectNode generateModelMapping(ObjectNode recordMap, ArrayNode targetModels,
                                                  String mappingId, String entityType, String schema) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode modelMapping = factory.objectNode();

        modelMapping.put("mappingId", mappingId);
        modelMapping.put("version", "1");
        String fullEntityType = "/source:string" + entityType;

        ObjectNode recordType = factory.objectNode();
        recordType.put("entityType", fullEntityType);
        recordType.put("format", "AVRO");
        recordType.put("schema", schema);

        modelMapping.set("recordType", recordType);
        modelMapping.set("recordMap", recordMap);
        modelMapping.set("targetModels", targetModels);

        return modelMapping;
    }

    public static ObjectNode generateModelMappingForEnhancement(ObjectNode recordMap, ArrayNode targetModels) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode modelMapping = factory.objectNode();

        modelMapping.set("recordMap", recordMap);
        modelMapping.set("targetModels", targetModels);

        return modelMapping;
    }
}
