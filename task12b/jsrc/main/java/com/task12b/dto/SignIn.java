package com.task12b.dto;

import org.json.JSONObject;

import java.util.Objects;

public final class SignIn {
    private final String email;
    private final String password;


    public SignIn(String email, String password) {
        if (email == null || password == null) {
            throw new IllegalArgumentException("Missing or incomplete data.");
        }
        this.email = email;
        this.password = password;
    }

    public static SignIn fromJson(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        String email = json.optString("email", null);
        String password = json.optString("password", null);

        return new SignIn(email, password);
    }

    public String email() {
        return email;
    }

    public String password() {
        return password;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SignIn) obj;
        return Objects.equals(this.email, that.email) &&
                Objects.equals(this.password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(email, password);
    }

    @Override
    public String toString() {
        return "SignIn[" +
                "email=" + email + ", " +
                "password=" + password + ']';
    }


}
