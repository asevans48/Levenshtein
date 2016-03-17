package com.hygenics.exceptions;

/**
 * Header Exception
 * 
 * @author aevans
 *
 */
public class NoHeaderException extends Exception {

	/**
	 * id
	 */
	private static final long serialVersionUID = -6350650010796419122L;

	public NoHeaderException() {
		super("No Headers Specified.");
	}

	public NoHeaderException(String Message) {
		super("No Headers Specified.\n" + Message);
	}

}
