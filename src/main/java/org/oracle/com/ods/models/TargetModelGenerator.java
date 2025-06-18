package org.oracle.com.ods.models;

import org.oracle.com.ods.util.DataTypeMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TargetModelGenerator {
    /**
     * Generates a JSON array of target models based on metadata and primary key information.
     *
     * @param metadata    List of column metadata maps.
     * @param tableName   Name of the table.
     * @param primaryKeys Set of primary key column names.
     * @return targetModelsArray ArrayNode representing the target models.
     */
    public static ArrayNode generateTargetModels(List<Map<String, Object>> metadata, String tableName, Set<String> primaryKeys,
                                                 Boolean isEnhancement) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ArrayNode columnsArray = factory.arrayNode();

        for (Map<String, Object> column : metadata) {
            ObjectNode columnObject = createColumnObject(column, primaryKeys);
            columnsArray.add(columnObject);
        }

        if (!isEnhancement) {
            ObjectNode rowVersionColumn = createRowVersionColumn();
            columnsArray.add(rowVersionColumn);
        }

        ObjectNode targetModel = factory.objectNode();
        targetModel.put("name", tableName);
        ArrayNode usesArray = factory.arrayNode();
        usesArray.add("DataSyndication");
        usesArray.add("Vertica");
        targetModel.set("uses", usesArray);
        targetModel.set("columns", columnsArray);

        ArrayNode targetModelsArray = factory.arrayNode();
        targetModelsArray.add(targetModel);

        return targetModelsArray;
    }

    /**
     * Creates an ObjectNode for a column based on metadata and primary key information.
     *
     * @param column      Map containing column metadata.
     * @param primaryKeys Set of primary key column names.
     * @return columnObject ObjectNode representing the column.
     */
    private static ObjectNode createColumnObject(Map<String, Object> column, Set<String> primaryKeys) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode columnObject = factory.objectNode();

        String columnName = (String) column.get("COLUMN_NAME");
        columnObject.put("name", columnName);

        ArrayNode usesArray = factory.arrayNode();
        usesArray.add("DataSyndication");
        usesArray.add("Vertica");
        if(primaryKeys != null && !primaryKeys.isEmpty()) {
            if (primaryKeys.contains(columnName)) {
                usesArray.add("PrimaryKey");
            }
        }
        columnObject.set("uses", usesArray);

        String dataType = (String) column.get("DATA_TYPE");
        columnObject.put("type", DataTypeMapper.mapDataType(dataType));
        columnObject.put("length", calculateLength(DataTypeMapper.mapDataType(dataType), (Integer) column.get("DATA_LENGTH")));
        columnObject.put("nullable", (Boolean) column.get("NULLABLE"));

        Object defaultValue = column.get("DATA_DEFAULT");
        if (defaultValue != null && !defaultValue.toString().isEmpty()) {
            String defaultValueStr = defaultValue.toString();
            columnObject.put("defaultValue", defaultValueStr);
        }

        return columnObject;
    }

    /**
     * Creates an ObjectNode for the constant column _ROW_VERSION.
     *
     * @return ObjectNode representing the _ROW_VERSION column.
     */
    private static ObjectNode createRowVersionColumn() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode rowVersionColumn = factory.objectNode();
        rowVersionColumn.put("name", "_ROW_VERSION");
        ArrayNode usesArray = factory.arrayNode();
        usesArray.add("Warehouse");
        usesArray.add("Version");
        rowVersionColumn.set("uses", usesArray);
        rowVersionColumn.put("type", "LONG");
        rowVersionColumn.put("nullable", false);
        rowVersionColumn.put("defaultValue", "0");
        return rowVersionColumn;
    }

    /**
     * Calculates the length of the column based on its data type and length.
     *
     * @param dataType Database data type.
     * @param dbLength Column length.
     * @return Calculated length.
     */
    private static int calculateLength(String dataType, int dbLength) {
        if ("STRING".equals(dataType) && dbLength >= 4000) {
            return 65000;
        }
        else if ("STRING".equals(dataType) || "TIMESTAMP".equals(dataType) || "FLOAT".equals(dataType) || "DOUBLE".equals(dataType) || "LONG".equals(dataType)) {
            return dbLength * 2;
        }
        return 0;
    }
}
