package com.task11.utils;

public class PasswordValidator {

    public static boolean validatePassword(String password) {
        return validateLength(password, 8, 20) &&
                containsUppercase(password) &&
                containsLowercase(password) &&
                containsDigit(password) &&
                containsSpecialCharacter(password);
    }

    private static boolean validateLength(String password, int minLength, int maxLength) {
        return password != null && password.length() >= minLength && password.length() <= maxLength;
    }

    private static boolean containsUppercase(String password) {
        return password != null && password.matches(".*[A-Z].*");
    }

    private static boolean containsLowercase(String password) {
        return password != null && password.matches(".*[a-z].*");
    }

    private static boolean containsDigit(String password) {
        return password != null && password.matches(".*\\d.*");
    }

    private static boolean containsSpecialCharacter(String password) {
        return password != null && password.matches(".*[!@#$%^&*()_+\\-=\\[\\]{};':\"\\\\|,.<>/?].*");
    }
}
