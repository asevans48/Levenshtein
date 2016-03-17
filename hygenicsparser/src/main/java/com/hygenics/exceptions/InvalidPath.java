package com.hygenics.exceptions;

public class InvalidPath extends Exception {

	public InvalidPath() {
		super("Invalid Regex/XML Path.");
	}

	public InvalidPath(String message) {
		super("Invalid Regex/XML." + message);
	}
}
