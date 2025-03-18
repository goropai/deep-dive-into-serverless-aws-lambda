package com.task12b.exceptions;

public class NoTableException extends RuntimeException {
    private final int code = 400;
    private final String message = "No table with such Id is found";

}
