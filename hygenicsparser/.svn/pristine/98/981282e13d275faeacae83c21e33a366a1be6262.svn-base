package com.hygenics.sort;

import java.util.ArrayList;

import com.hygenics.comparator.CompareObjects;

/**
 * Generic general purpose QuickSort. Sorts any list. O(nlogn) or O(n^2) but can
 * run smaller data better than heap or merge sorts.
 * 
 * @author asevans
 *
 * @param <E>
 */
public class Quicksort<E> {

	private ArrayList<E> tosortlist;

	private CompareObjects<E> comparator = new CompareObjects<E>();

	public Quicksort() {

	}

	public ArrayList<E> getTosortlist() {
		return tosortlist;
	}

	public void setTosortlist(ArrayList<E> tosortlist) {
		// sort list
		this.tosortlist = tosortlist;
	}

	/**
	 * Exchange two positions
	 */
	private void exchange(int newhigh, int newlow) {
		// TODO exchange positions in our list
		E temp = tosortlist.get(newhigh);
		tosortlist.set(newhigh, tosortlist.get(newlow));
		tosortlist.set(newlow, temp);
		temp = null;
	}

	/**
	 * Perform the quicksort
	 * 
	 * @param high
	 * @param low
	 */
	private void run(int inhigh, int inlow) {
		// TODO run the quicksort

		int high = inhigh;
		int low = inlow;
		int pivot = (int) Math.ceil((low + high) / 2);

		while (pivot > tosortlist.size()) {
			pivot--;
		}

		while (low <= high) {

			// get the splits
			while (comparator.compare(tosortlist.get(high),
					tosortlist.get(pivot)) > 0) {

				high--;
			}
			while (comparator.compare(tosortlist.get(low),
					tosortlist.get(pivot)) < 0) {
				low++;
			}

			if (low <= high) {
				exchange(high, low);
				low++;
				high--;
			}
		}

		// recursively get new list
		if (low < inhigh) {
			run(inhigh, low);
		}

		if (high > inlow) {
			run(high, inlow);
		}

	}

	/**
	 * Run the quicksort
	 */
	private ArrayList<E> run() {

		if (tosortlist != null) {
			if (tosortlist.size() > 0) {
				run((tosortlist.size() - 1), 0);
			}
		} else {
			tosortlist = new ArrayList<E>();
		}

		// return our new sorted list
		return tosortlist;
	}

	/**
	 * Perform the sort
	 * 
	 * @return
	 */
	public ArrayList<E> sort() {

		if (tosortlist == null) {
			try {
				throw new NullPointerException("No List to QuckSort!");
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		} else if (tosortlist.size() < 2) {
			return tosortlist;
		} else if (tosortlist.size() == 2) {
			CompareObjects<E> com = new CompareObjects<E>();

			if (com.compare(tosortlist.get(0), tosortlist.get(1)) == 1) {
				E temp = tosortlist.get(0);
				tosortlist.set(0, tosortlist.get(1));
				tosortlist.set(1, temp);
			}

			return tosortlist;
		}

		return run();
	}

}
