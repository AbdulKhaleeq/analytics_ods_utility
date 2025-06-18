package org.oracle.com.ods.services.mappingServices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Response;
import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.oracle.com.ods.services.utility.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class provides functionality to delete a latest version of a mapping in the development or staging environment.
 * The mapping is identified by its ID and version number.
 */
public class BatchDeleteMappings {

    private static final Logger logger = LoggerFactory.getLogger(BatchDeleteMappings.class);

    /**
     * Main method that prompts the user for a mapping ID
     * then attempts to delete the corresponding mapping from the selected environment(s).
     *
     * @param args Command line arguments (not used)
     * @throws IOException If there is an issue with the HTTP request
     */
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the mapping ids (comma-separated): ");
        String mappingIdsInput = scanner.nextLine();

        if (mappingIdsInput == null || mappingIdsInput.trim().isEmpty()) {
            logger.error("Mapping IDs cannot be empty. Exiting...");
            return;
        }

        System.out.print("Which service to post the model mapping? (dev, staging, both): ");
        String serviceChoice = scanner.nextLine().trim().toLowerCase();

        boolean postToDev = serviceChoice.equals("dev") || serviceChoice.equals("both");
        boolean postToStaging = serviceChoice.equals("staging") || serviceChoice.equals("both");

        List<String> mappingIds = Arrays.asList(mappingIdsInput.split(","));
        mappingIds.replaceAll(String::trim);

        if (postToDev) {
            String devToken = HttpClientHelper.getDevOAuthToken();
            processMappings(mappingIds, devToken, "dev");
        }

        if (postToStaging) {
            String stagingToken = HttpClientHelper.getStagingOAuthToken();
            processMappings(mappingIds, stagingToken, "staging");
        }
    }

    /**
     * Process mappings for the specified environment.
     *
     * @param mappingIds The list of mapping IDs
     * @param token      The OAuth token for authentication
     * @param environment The environment (dev or staging)
     * @throws IOException If there is an issue with the HTTP request
     */
    private static void processMappings(List<String> mappingIds, String token, String environment) throws IOException {
        String baseUrl = environment.equals("dev") ? Config.getDevModelMappingUrl() : Config.getStagingModelMappingUrl();
        SchemaType schemaType = environment.equals("dev") ? SchemaType.VERTICA : SchemaType.SNOWFLAKE;

        for (String mappingId : mappingIds) {
            if (mappingId.isEmpty()) continue;

            String latestVersion = getLatestVersionForMapping(mappingId, token, baseUrl);

            if (latestVersion != null) {
                String extractedId = HttpClientHelper.extractIdForMapping(mappingId, latestVersion, token, schemaType);
                if (extractedId != null && !extractedId.trim().isEmpty()) {
                    deleteMapping(mappingId, latestVersion, extractedId, token, baseUrl);
                } else {
                    logger.error("Couldn't find the record for mapping ID {} and version {}. Skipping...", mappingId, latestVersion);
                }
            } else {
                logger.error("Failed to fetch the latest version for mapping ID {}. Skipping...", mappingId);
            }
        }
    }

    /**
     * Fetch the latest version of the mapping for the given mapping ID.
     *
     * @param mappingId The mapping ID
     * @param token     OAuth token for authentication
     * @param baseUrl   The base URL of the environment (dev or staging)
     * @return The latest version as a String, or null if not found
     */
    private static String getLatestVersionForMapping(String mappingId, String token, String baseUrl) {
        try {
            List<String> versions = getMappingVersions(mappingId, token, baseUrl);

            if (!versions.isEmpty()) {
                OptionalInt maxVersion = versions.stream()
                        .mapToInt(Integer::parseInt)
                        .max();

                return String.valueOf(maxVersion.getAsInt());
            }
        } catch (Exception e) {
            logger.error("Error fetching the latest version for mapping ID {}: {}", mappingId, e.getMessage());
        }
        return null;
    }

    private static List<String> getMappingVersions(String mappingId, String token, String baseUrl) throws IOException {
        boolean useProxy = !baseUrl.equals(Config.getDevModelMappingUrl());
        String jsonResponse = HttpClientHelper.get(baseUrl, Map.of("mapping_id", mappingId), token, useProxy);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonResponse);

        JsonNode itemsNode = rootNode.get("items");

        List<String> versions = new ArrayList<>();

        if (itemsNode.isArray()) {
            for (JsonNode itemNode : itemsNode) {
                String version = itemNode.get("version").asText();
                versions.add(version);
            }
        }
        return versions;
    }

    /**
     * Delete the mapping with the given ID and version.
     *
     * @param mappingId     The mapping ID
     * @param version       The version to be deleted
     * @param extractedId   The internal ID for deletion
     * @param token         OAuth token for authentication
     * @param baseUrl       The base URL of the environment (dev or staging)
     * @throws IOException  If an issue occurs with the deletion request
     */
    private static void deleteMapping(String mappingId, String version, String extractedId, String token, String baseUrl) throws IOException {
        boolean useProxy = !baseUrl.equals(Config.getDevModelMappingUrl());
        String env = useProxy ? "Staging" : "Dev";

        try (Response response = HttpClientHelper.delete(baseUrl, extractedId, token, useProxy)) {
            if (response.code() == 204) {
                logger.info("Mapping ID {} with version {} deleted successfully from {} environment", mappingId, version, env);
            } else {
                String responseBody = response.body() != null ? response.body().string() : "No response body";
                throw new IOException("Failed to delete schema. Unexpected code " + response.code() + ": " + response.message() + " - " + responseBody);
            }
        }
    }
}
