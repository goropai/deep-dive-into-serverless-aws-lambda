package com.task12b.dto;

import org.json.JSONObject;

import java.util.Objects;

public final class SignUp {
    private final String email;
    private final String password;
    private final String firstName;
    private final String lastName;


    public SignUp(String email, String password, String firstName, String lastName) {
        if (email == null || password == null || firstName == null || lastName == null) {
            throw new IllegalArgumentException("Missing or incomplete data.");
        }
        this.email = email;
        this.password = password;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public static SignUp fromJson(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        String email = json.optString("email", null);
        String password = json.optString("password", null);
        String firstName = json.optString("firstName", null);
        String lastName = json.optString("lastName", null);

        return new SignUp(email, password, firstName, lastName);
    }

    public String email() {
        return email;
    }

    public String password() {
        return password;
    }

    public String firstName() {
        return firstName;
    }

    public String lastName() {
        return lastName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SignUp) obj;
        return Objects.equals(this.email, that.email) &&
                Objects.equals(this.password, that.password) &&
                Objects.equals(this.firstName, that.firstName) &&
                Objects.equals(this.lastName, that.lastName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(email, password, firstName, lastName);
    }

    @Override
    public String toString() {
        return "SignUp[" +
                "email=" + email + ", " +
                "password=" + password + ", " +
                "firstName=" + firstName + ", " +
                "lastName=" + lastName + ']';
    }


}
