package org.oracle.com.ods.services.schemaServices.snowflake;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

/**
 * This class handles the creation of a new Snowflake schema by interacting with
 * a remote service via an HTTP POST request. The schema details are provided by the user.
 */
public class CreateSnowflakeSchema {

    private static final Logger logger = LoggerFactory.getLogger(CreateSnowflakeSchema.class);

    /**
     * Main method that prompts the user for the schema name and initiates the schema creation process.
     *
     * @param args Command line arguments (not used)
     * @throws IOException If there is an issue with the HTTP request
     */
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the new Snowflake schema name: ");
        String schemaName = scanner.nextLine().trim();

        if (schemaName.isEmpty()) {
            logger.error("Schema name cannot be empty. Exiting...");
            return;
        }

        createSnowflakeSchema(schemaName);
    }

    /**
     * Method to create a new Snowflake schema by sending an HTTP POST request with the schema details.
     *
     * @param schemaName The name of the schema to be created
     */
    private static void createSnowflakeSchema(String schemaName) {
        String token = HttpClientHelper.getStagingOAuthToken();

        ObjectMapper objectMapper = new ObjectMapper();

        ObjectNode snowflakeSchemaBody = objectMapper.createObjectNode()
                .put("name", schemaName)
                .put("schemaType", "MILLENNIUM")
                .put("tenantId", "1c243940-7f30-46a7-94b9-965466a7fefd")
                .put("isTenantVisible", false)
                .put("etlLoadBehavior", "BY_NAME_ONLY")
                .put("readerRole", "STAGING_TEST_BLR_ANALYTICS_reader")
                .put("writerAccountId", "511e1b9c-565f-41d1-9d95-cfa3be5ca75d")
                .put("warehouseId", "904c4a4b-65ed-4401-8619-a58fd71a84f2");

        ObjectNode schemaMetadataItem = objectMapper.createObjectNode()
                .put("name", "source")
                .put("value", "e2de4a37-dfa5-4e92-83ee-ee1610806a06#all");

        ArrayNode schemaMetadataArray = objectMapper.createArrayNode();
        schemaMetadataArray.add(schemaMetadataItem);

        snowflakeSchemaBody.set("schemaMetadata", schemaMetadataArray);

        try {
            HttpClientHelper.post(Config.getSnowflakeSchemaUrl(), snowflakeSchemaBody, token, true);
            logger.info("Schema Created Successfully: {}", schemaName);
        } catch (IOException e) {
            logger.error("Failed to create schema: {}", schemaName, e);
        }
    }
}
