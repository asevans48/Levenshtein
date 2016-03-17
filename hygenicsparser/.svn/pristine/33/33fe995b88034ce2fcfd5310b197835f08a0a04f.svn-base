package com.hygenics.exceptions;

public class FTPFailureError extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	FTPFailureError() {
		super("Failed Connection");
	}

	FTPFailureError(int errorcode) {
		super("Failed to Connect! \n Error Code: " + errorcode);
	}

	FTPFailureError(String e) {
		super("Failure at FTP! \n Description Follows: \n" + e);
	}

}