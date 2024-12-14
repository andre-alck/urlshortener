package com.asacxyz;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class App implements RequestHandler<Map<String, Object>, Map<String, String>> {

  @Override
  public Map<String, String> handleRequest(Map<String, Object> input, Context context) {
    Object requestJsonContentString = input.get("body");
    if (requestJsonContentString == null) {
      return new HashMap<>();
    }

    Map<String, String> requestJsonContent = this.buildPayloadFromBody(requestJsonContentString.toString());
    UrlData urlData = this.buildUrlDataToPut(requestJsonContent);
    String shortenedUrl = UUID.randomUUID().toString().substring(0, 8);

    this.putObject(urlData, shortenedUrl);

    return Map.of("code", shortenedUrl);
  }

  private Map<String, String> buildPayloadFromBody(String body) {
    Map<String, String> payload = new HashMap<>();
    try {
      payload = new ObjectMapper().readValue(body, TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, String.class));
    } catch (Exception ignore) {
    }

    return payload;
  }

  private UrlData buildUrlDataToPut(Map<String, String> requestJsonContent) {
    String originalUrl = requestJsonContent.get("originalUrl");
    String expirationTime = String.valueOf(requestJsonContent.get("expirationTime"));
    long expirationTimeInSeconds = Long.valueOf(expirationTime) * 3600;
    return new UrlData(originalUrl, expirationTimeInSeconds);
  }

  private void putObject(UrlData urlData, String shortenedUrl) {
    String json = null;
    try {
      json = new ObjectMapper().writeValueAsString(urlData);
    } catch (Exception ignore) {
    }

    S3Client s3 = S3Client.builder().build();
    PutObjectRequest bucketInfo = PutObjectRequest.builder().bucket("urlshortener-s3-asacyxz").key(shortenedUrl + ".json").build();
    s3.putObject(bucketInfo, RequestBody.fromString(json));
  }
}