package com.asacxyz;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class App implements RequestHandler<Map<String, Object>, Map<String, String>> {
    private static final String BODY_KEY = "body";
    private static final String ERROR_VALUE = "error";
    private static final String SHORTENED_URL_KEY = "code";
    private static final String BUCKET_NAME = "urlshortener-s3-asacxyz";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, String> handleRequest(Map<String, Object> input, Context context) {
        Object requestJsonContentString = input.get(App.BODY_KEY);
        if (requestJsonContentString == null) {
            return this.buildErrorResponse("Request did not receive a body.");
        }

        try {
            Map<String, String> requestJsonContent = this.buildPayloadFromBody(requestJsonContentString.toString());
            UrlData urlData = this.buildUrlDataToPut(requestJsonContent);
            String shortenedUrl = UUID.randomUUID().toString().substring(0, 8);
            this.putObject(urlData, shortenedUrl);
            return this.buildResponse(shortenedUrl);
        } catch (Exception e) {
            return this.buildErrorResponse(e);
        }
    }

    private Map<String, String> buildErrorResponse(Object e) {
        this.log(e);
        return this.buildResponse(App.ERROR_VALUE);
    }

    private void log(Object e) {
        if (e instanceof Exception) {
            ((Exception) e).printStackTrace();
            return;
        }

        System.out.println(e);
    }

    private Map<String, String> buildResponse(String shortenedUrlCode) {
        return Map.of(App.SHORTENED_URL_KEY, shortenedUrlCode);
    }

    private Map<String, String> buildPayloadFromBody(String body) throws JsonProcessingException {
        return this.objectMapper.readValue(body, TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, String.class));
    }

    private UrlData buildUrlDataToPut(Map<String, String> requestJsonContent) throws Exception {
        String originalUrl = requestJsonContent.get("originalUrl");
        String expirationTimeString = requestJsonContent.get("expirationTime");

        if (originalUrl == null || originalUrl.isBlank() || expirationTimeString == null || expirationTimeString.isBlank()) {
            throw new Exception("Necessary parameters were missing.");
        }

        long expirationTime = Long.valueOf(expirationTimeString);
        return new UrlData(originalUrl, expirationTime);
    }

    private void putObject(UrlData urlData, String shortenedUrl) throws JsonProcessingException {
        String json = this.objectMapper.writeValueAsString(urlData);
        PutObjectRequest bucketInfo = PutObjectRequest.builder().bucket(App.BUCKET_NAME).key(shortenedUrl + ".json").build();
        try (S3Client s3 = S3Client.builder().build()) {
            s3.putObject(bucketInfo, RequestBody.fromString(json));
        } catch (Exception e) {
            this.log(e);
        }
    }
}