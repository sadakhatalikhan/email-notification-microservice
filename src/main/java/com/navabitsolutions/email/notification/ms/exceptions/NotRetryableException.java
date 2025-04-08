package com.navabitsolutions.email.notification.ms.exceptions;

public class NotRetryableException extends Exception {
    public NotRetryableException(String message) {
        super(message);
    }
    public NotRetryableException(Throwable throwable) {
        super(throwable);
    }
}
