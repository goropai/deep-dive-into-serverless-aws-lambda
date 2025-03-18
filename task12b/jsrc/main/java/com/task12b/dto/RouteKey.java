package com.task12b.dto;

import java.util.Objects;

public final class RouteKey {
    private final String method;
    private final String path;

    public RouteKey(String method, String path) {
        this.method = method;
        this.path = path;
    }

    public String method() {
        return method;
    }

    public String path() {
        return path;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RouteKey) obj;
        return Objects.equals(this.method, that.method) &&
                Objects.equals(this.path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, path);
    }

    @Override
    public String toString() {
        return "RouteKey[" +
                "method=" + method + ", " +
                "path=" + path + ']';
    }


}
