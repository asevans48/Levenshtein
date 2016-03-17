package com.hygenics.exceptions;

public class BadRegex extends Exception {

	public BadRegex() {
		super("No Results from Regex, Cannot Proceed");
	}

	public BadRegex(String e) {
		super("No Results from Regex, Cannot Proceed:" + e);
	}
}
