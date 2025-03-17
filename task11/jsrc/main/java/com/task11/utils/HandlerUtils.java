package com.task11.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

public class HandlerUtils {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static Map<String, Object> jsonToMap(String object) {
        try {
            Map<String, Object> map = objectMapper.readValue(object, Map.class);
            return map;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON cannot be converted to Map: " + object);
        }
    }

    public static Map<String, String> requestToMap(Object request) {
        try {
            Map<String, String> map = objectMapper.readValue(convertObjectToJson(request), LinkedHashMap.class);
            return map;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON cannot be converted to Map: " + request);
        }
    }
    public static Map<String, Object> requestToObjectMap(Object request) {
        try {
            Map<String, Object> map = objectMapper.readValue(convertObjectToJson(request), LinkedHashMap.class);
            return map;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON cannot be converted to Map: " + request);
        }
    }

    public static String convertObjectToJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Object cannot be converted to JSON: " + object);
        }
    }

    public static String getFormattedTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        String createdAt = OffsetDateTime.now(ZoneOffset.UTC).format(formatter);
        return createdAt;
    }
}
