package com.hygenics.exceptions;

/**
 * Parameter Exception
 * 
 * @author aevans
 *
 */
public class NoParameterException extends Exception {

	/**
	 * id
	 */
	private static final long serialVersionUID = 8169090334362128012L;

	public NoParameterException() {
		super("No Parameters Specified For Post.");
	}

	public NoParameterException(String message) {
		super("No Parameters Specified for Post.\n" + message);
	}

}
