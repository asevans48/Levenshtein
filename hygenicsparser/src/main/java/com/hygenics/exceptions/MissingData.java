package com.hygenics.exceptions;

public class MissingData extends Exception {

	/**
	 * id
	 */
	private static final long serialVersionUID = 5337996038841230855L;

	public MissingData() {
		super("Missing Critical Data");
	}

	public MissingData(String e) {
		super("Missing Critical Data:" + e);
	}

}
