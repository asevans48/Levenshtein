package com.hygenics.exceptions;

public class PortReservedException extends Exception {

	/**
	 * id
	 */
	private static final long serialVersionUID = 4055706597578602054L;

	public PortReservedException() {
		super("Reserved or Invalid Port");
	}

	public PortReservedException(String e) {
		super("Reserved or Invalid Port" + e);
	}
}
