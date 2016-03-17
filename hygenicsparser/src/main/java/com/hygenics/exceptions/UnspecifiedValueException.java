package com.hygenics.exceptions;

public class UnspecifiedValueException extends Exception {

	private static final long serialVersionUID = -1154925454660186273L;

	public UnspecifiedValueException() {
		super(
				"A Value is Missing. Perhaps a value that was supposed to be set was Null.");
	}
}
