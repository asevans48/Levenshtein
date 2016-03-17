package com.hygenics.exceptions;

public class MismatchException extends Exception {

	private static final long serialVersionUID = -6862838020754367124L;

	public MismatchException() {
		super("Objects do not match in a key aspect!");
	}

	public MismatchException(String message) {
		super("Objects do not mathc in a key aspect: \n " + message);
	}

}
