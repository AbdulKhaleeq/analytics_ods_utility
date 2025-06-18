package org.oracle.com.ods.services.mappingServices;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

/**
 * This class handles posting a model mapping to specified services (Vertica, Snowflake, or both).
 * It collects required details from the user, reads the model mapping document from a file,
 * and sends the JSON payload to the selected services.
 */
@Deprecated
public class PostMappings {

    private static final Logger logger = LoggerFactory.getLogger(PostMappings.class);

    /**
     * Main method that prompts the user for various details,
     * reads the model mapping document from a file, and posts the mapping to the selected services.
     *
     * @param args Command line arguments (not used)
     * @throws IOException If there is an issue with file reading or the HTTP request
     */
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter mapping_id: ");
        String mappingId = scanner.nextLine().trim();
        if (mappingId.isEmpty()) {
            logger.error("Mapping ID cannot be empty. Exiting...");
            return;
        }

        System.out.print("Enter version: ");
        String version = scanner.nextLine().trim();
        if (version.isEmpty()) {
            logger.error("Version cannot be empty. Exiting...");
            return;
        }

        System.out.print("Enter record_format: ");
        String recordFormat = scanner.nextLine().trim();
        if (recordFormat.isEmpty()) {
            logger.error("Record format cannot be empty. Exiting...");
            return;
        }

        System.out.print("Enter entity_type: ");
        String entityType = scanner.nextLine().trim();
        if (entityType.isEmpty()) {
            logger.error("Entity type cannot be empty. Exiting...");
            return;
        }

        System.out.print("Enter the file path for the model_mapping_document: ");
        String filePath = scanner.nextLine().trim();
        if (filePath.isEmpty()) {
            logger.error("File path for model_mapping_document cannot be empty. Exiting...");
            return;
        }

        String jsonContent;
        try {
            jsonContent = new String(Files.readAllBytes(Paths.get(filePath)));
            System.out.println("File Content successfully read.");
        } catch (IOException e) {
            logger.error("Failed to read the file content", e);
            return;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode jsonBody = objectMapper.createObjectNode();

        jsonBody.put("mapping_id", mappingId);
        jsonBody.put("version", version);
        jsonBody.put("record_format", recordFormat);
        jsonBody.put("entity_type", entityType);
        jsonBody.put("model_mapping_document", jsonContent);

        System.out.print("Which service to post the model mapping? (vertica, snowflake, both): ");
        String serviceChoice = scanner.nextLine().trim().toLowerCase();

        boolean postToVertica = serviceChoice.equals("vertica") || serviceChoice.equals("both");
        boolean postToSnowflake = serviceChoice.equals("snowflake") || serviceChoice.equals("both");

        if (postToVertica) {
            try {
                String token = HttpClientHelper.getDevOAuthToken();
                HttpClientHelper.post(Config.getDevModelMappingUrl(), jsonBody, token, false);
                logger.info("Model mapping posted successfully to Vertica.");
            } catch (IOException e) {
                logger.error("Failed to post model mapping to Vertica", e);
            }
        }

        if (postToSnowflake) {
            try {
                String token = HttpClientHelper.getStagingOAuthToken();
                HttpClientHelper.post(Config.getStagingModelMappingUrl(), jsonBody, token, true);
                logger.info("Model mapping posted successfully to Snowflake.");
            } catch (IOException e) {
                logger.error("Failed to post model mapping to Snowflake", e);
            }
        }

        if (!postToVertica && !postToSnowflake) {
            logger.warn("No valid service selected. Model mapping was not posted.");
        }
    }
}
