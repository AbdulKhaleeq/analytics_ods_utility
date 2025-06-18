package org.oracle.com.ods.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.oracle.com.ods.db.MetadataExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonCompactor {
    private static final Logger logger = LoggerFactory.getLogger(JsonCompactor.class);

    public static String compactAndEscapeJson(ObjectNode objectNode) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(objectNode);
        } catch (JsonProcessingException e) {
            logger.error("Error parsing JSON: {}", objectNode, e);
            return null;
        }
    }
}
