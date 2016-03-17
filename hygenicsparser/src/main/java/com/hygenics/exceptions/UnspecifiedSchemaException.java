package com.hygenics.exceptions;

public class UnspecifiedSchemaException extends Exception {

	private static final long serialVersionUID = -2646401951058906523L;

	public UnspecifiedSchemaException() {
		super("Schema was not Specified");
	}
}
