package com.hygenics.comparator;

public class CompareObjects<E> {

	public CompareObjects() {

	}

	/**
	 * Compare two generic values if o is < co -> retvalue=-1 else if o=co ->
	 * revalue=0 else retvalue 1
	 * 
	 * Implements and overrides Comparator to avoid instantiating a new class
	 * for every arraylist object
	 * 
	 * @param o
	 *            -compare
	 * @param co
	 *            -compare to
	 * @return revalue - the integer value representing the result
	 */
	public int compare(Object o, Object co) {
		// compare objects

		int retvalue = -2;

		if (o instanceof java.lang.String & co instanceof java.lang.String) {

			// compare strings
			String comp = (String) o;
			String compto = (String) co;
			return comp.compareToIgnoreCase(compto);
		} else {
			// compare numbers and characters
			if (o instanceof java.lang.Float) {
				if ((java.lang.Float) o == (java.lang.Float) co) {
					retvalue = 0;
				} else if ((java.lang.Float) o < (java.lang.Float) co) {
					retvalue = -1;
				} else {
					retvalue = 1;
				}

			} else if (o instanceof java.lang.Double
					& co instanceof java.lang.Double) {

				if ((java.lang.Float) o == (java.lang.Float) co) {
					retvalue = 0;
				} else if ((java.lang.Float) o < (java.lang.Float) co) {
					retvalue = -1;
				} else {
					retvalue = 1;
				}
			} else if (o instanceof java.lang.Character
					& co instanceof java.lang.Character) {

				if ((java.lang.Character) o == (java.lang.Character) co) {
					retvalue = 0;
				} else if ((java.lang.Character) o < (java.lang.Character) co) {
					retvalue = -1;
				} else {
					retvalue = 1;
				}
			} else if (o instanceof java.lang.Integer
					& co instanceof java.lang.Integer) {

				if ((java.lang.Integer) o == (java.lang.Integer) co) {
					retvalue = 0;
				} else if ((java.lang.Integer) o < (java.lang.Integer) co) {
					retvalue = -1;
				} else {
					retvalue = 1;
				}
			}

		}

		return retvalue;
	}

}
