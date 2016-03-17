package com.hygenics.exceptions;

public class TableMissingException extends Exception {

	private static final long serialVersionUID = 2362718990098285748L;

	public TableMissingException() {
		super();
	}

	public TableMissingException(String e) {
		super("Table Missing:\n" + e);
	}

}
