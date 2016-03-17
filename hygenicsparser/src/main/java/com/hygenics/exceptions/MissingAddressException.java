package com.hygenics.exceptions;

public class MissingAddressException extends Exception {

	/**
	 * id
	 */
	private static final long serialVersionUID = -5753844186042860565L;

	public MissingAddressException() {
		super("Missing LOCALHOST or SERVER Address");
	}

	public MissingAddressException(String e) {
		super("Missing LOCALHOST or SERVER Address" + e);
	}
}
