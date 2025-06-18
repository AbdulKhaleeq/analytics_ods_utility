package org.oracle.com.ods.services.utility;

import com.cerner.pophealth.programs.util.AuthHeader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.oracle.com.ods.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for handling HTTP requests and OAuth token management.
 */
public class HttpClientHelper {
    private static final Logger logger = LoggerFactory.getLogger(HttpClientHelper.class);
    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json");

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final OkHttpClient defaultClient = buildDefaultClient();
    private static final OkHttpClient proxyClient = buildProxyClient(Config.getProxyHost(), Config.getProxyPortStaging());


    private static OkHttpClient buildDefaultClient() {
        return new OkHttpClient.Builder()
                .connectTimeout(TIMEOUT)
                .writeTimeout(TIMEOUT)
                .readTimeout(TIMEOUT)
                .build();
    }

    private static OkHttpClient buildProxyClient(String proxyHost, int proxyPort) {
        return new OkHttpClient.Builder()
                .connectTimeout(TIMEOUT)
                .writeTimeout(TIMEOUT)
                .readTimeout(TIMEOUT)
                .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)))
                .build();
    }

    public static OkHttpClient createCustomProxyClient(String proxyHost, int proxyPort) {
        return buildProxyClient(proxyHost, proxyPort);
    }

    public static String get(String url, Map<String, String> params, String token, boolean useProxy) throws IOException {
        OkHttpClient client = shouldUseCustomProxy(url) ? createCustomProxyClient(Config.getProxyHost(), Config.getProxyPortSphereStage()) : getClient(useProxy);

        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();
        params.forEach(urlBuilder::addQueryParameter);

        Request request = buildRequest(urlBuilder.build().toString(), token)
                .get()
                .build();

        try (Response response = client.newCall(request).execute()) {
            return handleResponse(response);
        }
    }

    public static void post(String url, ObjectNode body, String token, boolean useProxy) throws IOException {
        OkHttpClient client = shouldUseCustomProxy(url) ? createCustomProxyClient(Config.getProxyHost(), Config.getProxyPortSphereStage()) : getClient(useProxy);
        RequestBody requestBody = RequestBody.create(body.toString(), JSON_MEDIA_TYPE);

        Request request = buildRequest(url, token)
                .post(requestBody)
                .build();

        try (Response response = client.newCall(request).execute()) {
            validateResponse(response);
        }
    }

    public static Response delete(String url, String objectId, String token, boolean useProxy) throws IOException {
        OkHttpClient client = getClient(useProxy);

        Request request = buildRequest(url + "/" + objectId, token)
                .delete()
                .build();

        return client.newCall(request).execute();
    }

    public static String getSchemaId(String url, String schemaName, String token, boolean useProxy, SchemaType schemaType) throws IOException {
        String responseBody = get(url, Map.of("name", schemaName), token, useProxy);

        if (schemaType == SchemaType.VERTICA) {
            JSONArray jsonArray = new JSONArray(responseBody);
            if (jsonArray.isEmpty()) {
                throw new IOException("No schema found with the name: " + schemaName);
            }
            JSONObject schemaObject = jsonArray.getJSONObject(0);
            return String.valueOf(schemaObject.getInt("id"));
        }
        else if (schemaType == SchemaType.SNOWFLAKE) {
            JSONObject jsonResponse = new JSONObject(responseBody);
            JSONArray itemsArray = jsonResponse.getJSONArray("items");
            if (itemsArray.isEmpty()) {
                throw new IOException("No schema found with the name: " + schemaName);
            }
            JSONObject schemaObject = itemsArray.getJSONObject(0);
            return schemaObject.getString("id");
        }
        else {
            throw new IllegalArgumentException("Unknown schema type: " + schemaType);
        }
    }

    public static void deleteSchema(String url, String schemaName, String schemaId, String token, boolean useProxy) throws IOException {
        try (Response response = delete(url, schemaId, token, useProxy)) {
            if (response.code() == 204) {
                logger.info("Schema {} deleted successfully with ID: {}", schemaName, schemaId);
            } else {
                handleUnexpectedResponse(response);
            }
        }
    }

    public static String extractIdForMapping(String mappingId, String targetVersion, String token, SchemaType service) throws IOException {
        String id = null;
        String stagingUrl = (service != SchemaType.VERTICA) ? Config.getStagingModelMappingUrl() : Config.getDevModelMappingUrl();

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("mapping_id", mappingId);

        String responseBody = get(stagingUrl, queryParams, token, service != SchemaType.VERTICA);

        JSONObject jsonResponse = new JSONObject(responseBody);
        JSONArray itemsArray = jsonResponse.getJSONArray("items");

        for(int i = 0; i < itemsArray.length(); i++) {
            JSONObject item = itemsArray.getJSONObject(i);
            String version = item.getString("version");

            if(version.equals(targetVersion)) {
                id = String.valueOf(item.getInt("id"));
                break;
            }
        }
        return id;
    }

    public static String getDevOAuthToken() {
        return getOauthToken(Config.getDevOAuthAccessUrl(), Config.getDevOAuthConsumerKey(), Config.getDevOAuthConsumerSecret());
    }

    public static String getStagingOAuthToken() {
        return getOauthToken(Config.getStagingOAuthAccessUrl(), Config.getStagingOAuthConsumerKey(), Config.getStagingOAuthConsumerSecret());
    }

    private static OkHttpClient getClient(boolean useProxy) {
        return useProxy ? proxyClient : defaultClient;
    }

    private static Request.Builder buildRequest(String url, String token) {
        return new Request.Builder()
                .url(url)
                .addHeader("Authorization", token);
    }

    private static void validateResponse(Response response) throws IOException {
        if (!response.isSuccessful()) {
            handleUnexpectedResponse(response);
        }
    }

    private static String handleResponse(Response response) throws IOException {
        if (!response.isSuccessful()) {
            handleUnexpectedResponse(response);
        }
        if (response.body() == null) {
            throw new IOException("Response body is null");
        }
        return response.body().string();
    }

    private static void handleUnexpectedResponse(Response response) throws IOException {
        String responseBody = response.body() != null ? response.body().string() : "No response body";
        throw new IOException("Unexpected code " + response.code() + ": " + response.message() + " - " + responseBody);
    }

    private static boolean shouldUseCustomProxy(String url) {
        return url.equalsIgnoreCase(Config.getStagingModelMappingUrl());
    }

    private static String getOauthToken(String url, String consumerKey, String consumerSecret) {
        try {
            return AuthHeader.getAuthorizationHeader(url, consumerKey, consumerSecret);
        } catch (Exception e) {
            logger.error("Error while getting OAuth token", e);
            return null;
        }
    }
}
