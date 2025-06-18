package org.oracle.com.ods.services.mappingServices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides functionality to post the model mappings in the development or staging environment services
 * for a given list of mappings ids.
 * The mapping is identified by its ID and version number.
 */
public class BatchPostMappings {
    private static final Logger logger = LoggerFactory.getLogger(BatchPostMappings.class);

    private static boolean postToDev;
    private static boolean postToStaging;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        String parentDirectory = Config.getModelMappingConfigPath();

        System.out.print("Enter the mapping ids (comma-separated): ");
        String mappingIdsInput = scanner.nextLine();

        System.out.print("Which service to post the model mapping? (dev, staging, both): ");
        String serviceChoice = scanner.nextLine().trim().toLowerCase();

         postToDev = serviceChoice.equals("dev") || serviceChoice.equals("both");
         postToStaging = serviceChoice.equals("staging") || serviceChoice.equals("both");

        String[] mappingIds = mappingIdsInput.split(",");
        for (String mappingId : mappingIds) {
            processAndPostMapping(parentDirectory, mappingId.trim());
        }
    }

    /**
     * Reads the model mappings from the given path for the given mappingId.
     *
     * @param parentDirectory The Path for model mappings
     * @param mappingId      The mapping id to be processed
     * @throws IOException If there is an issue with the HTTP request
     */
    private static void processAndPostMapping(String parentDirectory, String mappingId) throws IOException {
        File mappingFile = new File(parentDirectory + "/" + mappingId + ".json");
        if (!mappingFile.exists()) {
            System.out.println("Mapping file not found for ID: " + mappingId);
            return;
        }

        String fileContent = new String(Files.readAllBytes(Paths.get(mappingFile.getPath())));
        JsonNode jsonNode = objectMapper.readTree(fileContent);

        String version = jsonNode.get("version").asText();
        String entityType = jsonNode.get("recordType").get("entityType").asText();
        String recordFormat = jsonNode.get("recordType").get("format").asText();

        String compressedContent = fileContent.replaceAll("\\s+", "");

        ObjectNode jsonBody = objectMapper.createObjectNode();

        jsonBody.put("mapping_id", mappingId);
        jsonBody.put("version", version);
        jsonBody.put("record_format", recordFormat);
        jsonBody.put("entity_type", entityType);
        jsonBody.put("model_mapping_document", compressedContent);

        postModelMapping(jsonBody);
        System.out.println(jsonBody);
    }

    /**
     * Posts the model mappings for the given mappingId into the chosen environment.
     *
     * @param postBody The Body for model mappings to be posted
     */
    private static void postModelMapping(ObjectNode postBody) {
        if (postToDev) {
            try {
                String token = HttpClientHelper.getDevOAuthToken();
                HttpClientHelper.post(Config.getDevModelMappingUrl(), postBody, token, false);
                logger.info("Model mapping posted successfully to Vertica.");
            } catch (IOException e) {
                logger.error("Failed to post model mapping to Vertica", e);
            }
        }

        if (postToStaging) {
            try {
                String token = HttpClientHelper.getStagingOAuthToken();
                HttpClientHelper.post(Config.getStagingModelMappingUrl(), postBody, token, true);
                logger.info("Model mapping posted successfully to Snowflake.");
            } catch (IOException e) {
                logger.error("Failed to post model mapping to Snowflake", e);
            }
        }

        if (!postToDev && !postToStaging) {
            logger.warn("No valid service selected. Model mapping was not posted.");
        }
    }
}
