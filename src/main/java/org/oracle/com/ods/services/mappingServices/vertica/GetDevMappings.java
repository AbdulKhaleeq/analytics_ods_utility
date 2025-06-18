package org.oracle.com.ods.services.mappingServices.vertica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.json.JSONException;
import org.oracle.com.ods.config.Config;
import org.oracle.com.ods.services.utility.HttpClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

/**
 * This class provides functionality to fetch and display a specific mapping from the development environment.
 * The mapping is identified by its ID.
 */
public class GetDevMappings {

    private static final Logger logger = LoggerFactory.getLogger(GetDevMappings.class);

    /**
     * Main method that prompts the user for a mapping ID,
     * then retrieves and prints the corresponding mapping in a pretty JSON format.
     *
     * @param args Command line arguments (not used)
     * @throws IOException If there is an issue with the HTTP request or processing the JSON response
     */
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        // Prompt the user for the mapping ID
        System.out.print("Enter the mapping id: ");
        String mappingId = scanner.nextLine();

        if (mappingId == null || mappingId.trim().isEmpty()) {
            logger.error("Mapping ID cannot be empty. Exiting...");
            return;
        }

        String token = HttpClientHelper.getDevOAuthToken();

        try {
            String responseBody = HttpClientHelper.get(Config.getDevModelMappingUrl(), Map.of("mapping_id", mappingId), token, false);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonResponse = objectMapper.readTree(responseBody);

            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
            String prettyJson = objectMapper.writeValueAsString(jsonResponse);

            System.out.println("Service Response (Pretty Format):");
            System.out.println(prettyJson);
        } catch (JSONException e) {
            logger.error("Couldn't fetch the mapping for mapping id: {}", mappingId, e);
        }
    }
}
