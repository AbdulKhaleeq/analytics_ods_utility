package org.oracle.com.ods.models;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;

public class RecordMapGenerator {
    /**
     * Generates a JSON object representing the record map for a given table and metadata.
     *
     * @param metadata   List of column metadata maps.
     * @param tableName  Name of the table.
     * @param recordId   Unique identifier for the record.
     * @return recordMap ObjectNode representing the record map.
     */
    public static ObjectNode generateRecordMap(List<Map<String, Object>> metadata, String tableName, String recordId) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ArrayNode columnMapsArray = factory.arrayNode();

        for (Map<String, Object> column : metadata) {
            ObjectNode columnMap = factory.objectNode();
            String columnName = (String) column.get("COLUMN_NAME");
            String fieldName = columnName != null ? columnName.toLowerCase() : "";

            columnMap.put("columnName", columnName);
            columnMap.put("fieldName", fieldName);
            columnMap.put("recordId", recordId);

            columnMapsArray.add(columnMap);
        }

        ObjectNode targetMap = factory.objectNode();
        targetMap.put("targetName", tableName);
        targetMap.set("columnMaps", columnMapsArray);

        ArrayNode targetMapsArray = factory.arrayNode();
        targetMapsArray.add(targetMap);

        ObjectNode recordMap = factory.objectNode();
        recordMap.put("recordId", recordId);
        recordMap.set("targetMaps", targetMapsArray);

        return recordMap;
    }
}
