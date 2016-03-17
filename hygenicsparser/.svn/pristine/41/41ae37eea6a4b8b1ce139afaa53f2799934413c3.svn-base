package com.hygenics.sort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Calls the Java Merge Sort. In here just as another option. Merge sort
 * surpasses quick sort in performance after an array reaches a large size.
 * 
 * Why remake it?
 * 
 * @author aevens
 *
 * @param <E>
 */
public class MergeSort<E> {

	private ArrayList<E> tosortlist;

	public MergeSort() {

	}

	/**
	 * Performs Conversion and sorting
	 */
	private ArrayList<E> sort() {

		// get the type and then sort
		if (tosortlist.size() < 1) {
			return tosortlist;
		} else if (tosortlist.get(0) instanceof java.lang.Integer) {
			List<Integer> list = (List<Integer>) tosortlist.subList(0,
					(tosortlist.size() - 1));

			tosortlist.clear();
			Collections.sort(list);

			for (Integer i : list) {
				tosortlist.add((E) i);
			}
		} else if (tosortlist.get(0) instanceof java.lang.Double) {
			List<Double> list = (List<Double>) tosortlist.subList(0,
					(tosortlist.size() - 1));

			tosortlist.clear();
			Collections.sort(list);

			for (Double d : list) {
				tosortlist.add((E) d);
			}
		} else if (tosortlist.get(0) instanceof java.lang.Float) {
			List<Float> list = (List<Float>) tosortlist.subList(0,
					(tosortlist.size() - 1));

			tosortlist.clear();
			Collections.sort(list);

			for (Float f : list) {
				tosortlist.add((E) f);
			}

		} else if (tosortlist.get(0) instanceof java.lang.Character) {
			List<Character> list = (List<Character>) tosortlist.subList(0,
					(tosortlist.size() - 1));

			tosortlist.clear();
			Collections.sort(list);

			for (Character c : list) {
				tosortlist.add((E) c);
			}
		} else if (tosortlist.get(0) instanceof java.lang.String) {
			List<String> list = (List<String>) tosortlist.subList(0,
					(tosortlist.size() - 1));

			tosortlist.clear();
			Collections.sort(list);

			for (String s : list) {
				tosortlist.add((E) s);
			}
		}

		return tosortlist;

	}

	public ArrayList<E> run() {

		if (tosortlist == null) {
			try {
				throw new NullPointerException("No list provided");
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		}

		return sort();
	}

}
