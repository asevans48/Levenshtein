package com.hygenics.exceptions;

public class NoClassSpecified extends Exception {

	private static final long serialVersionUID = 2089563121863787939L;

	public NoClassSpecified() {
		super("No Type Specified.");
	}

	public NoClassSpecified(String e) {
		super(e);
	}
}
