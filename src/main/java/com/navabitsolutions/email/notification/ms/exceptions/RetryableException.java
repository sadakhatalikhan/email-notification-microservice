package com.navabitsolutions.email.notification.ms.exceptions;

public class RetryableException extends RuntimeException {
    public RetryableException(String message) {
        super(message);
    }
    public RetryableException(Throwable throwable) {
        super(throwable);
    }
}
