package com.hygenics.exceptions;

public class DataMalformedException extends Exception {

	public DataMalformedException() {
		super(Messages.getString("DataMalformedException.0")); //$NON-NLS-1$
	}

	public DataMalformedException(String e) {
		super(Messages.getString("DataMalformedException.1") + e); //$NON-NLS-1$
	}

}
