package com.hygenics.exceptions;

public class NoRowSpecifiedException extends Exception {

	private static final long serialVersionUID = -4105676253624985943L;

	public NoRowSpecifiedException() {
		super("No Row Specified");
	}
}
