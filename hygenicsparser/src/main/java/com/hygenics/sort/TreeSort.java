package com.hygenics.sort;

import java.util.ArrayList;

import com.hygenics.comparator.CompareObjects;

/**
 * Another sorta big data sort. This is a tree sort. It creates a binary tree
 * from the data. You can get the treenode data or a sorted array. Worst case is
 * O(n^2) but best case is O(nlogn) in sorting. This is mainly so I can obtain
 * quick access to a tree. The programmer needs to call getList to print a
 * sorted list.
 * 
 * I may move this out later to deal with A*, SMA*, Alpha-Beta, BFS; DFS finding
 * but HD doesn't seem like the place for BFS and DFS path finding and SMA,
 * alpha-beat and A * searches would help in matching if fruitful.
 * 
 * Don't worry this class was done when running a ton of other programs to check
 * their returns and after 5:30 pm.
 * 
 * @author aevens
 *
 */
public class TreeSort<E> {

	private ArrayList<E> tosortlist;
	private TreeNode head = null;
	private TreeNode current = null;
	private CompareObjects<E> comparator = new CompareObjects<E>();

	public TreeSort() {

	}

	/**
	 * Returns the head node data
	 * 
	 * @return
	 */
	public Object gethead() {
		return head.data;
	}

	/**
	 * set tree left
	 */
	public void setLeft() {
		current = current.getLeft();
	}

	/**
	 * Set a Node Right
	 */
	public void setRight() {
		current = current.getRight();
	}

	/**
	 * Reset Current to Head
	 */
	public void resetCurrent() {
		current = head;
	}

	/**
	 * Returns the arraylist
	 * 
	 * @return
	 */
	public ArrayList<E> getList() {
		return InOrderTraversal();
	}

	private class TreeNode<E> {

		private boolean visited = false;
		private TreeNode<E> head;
		private TreeNode<E> right;
		private TreeNode<E> left;
		private TreeNode<E> parent;
		private E data;

		protected TreeNode(E data, TreeNode<E> head) {
			this.head = head;
			this.data = data;

		}

		public TreeNode<E> getHead() {
			return head;
		}

		public void setHead(TreeNode<E> head) {
			this.head = head;
		}

		public E getData() {
			return data;
		}

		public void setData(E data) {
			this.data = data;
		}

		public TreeNode<E> getRight() {
			return right;
		}

		public void setRight(TreeNode<E> right) {
			this.right = right;
		}

		public TreeNode<E> getLeft() {
			return left;
		}

		public void setLeft(TreeNode<E> left) {
			this.left = left;
		}

		public TreeNode<E> getParent() {
			return parent;
		}

		public void setParent(TreeNode<E> parent) {
			this.parent = parent;
		}

		public boolean isVisited() {
			return visited;
		}

		public void setVisited(boolean visited) {
			this.visited = visited;
		}

	}

	/**
	 * The recursive element for adding a node
	 * 
	 * @param current
	 * @param newNode
	 */
	private void addNode(TreeNode<E> current, TreeNode<E> newNode) {

		if (comparator.compare(newNode.data, current.data) <= 0) {
			// the new node is < the current node go left or add
			if (current.getRight() != null) {
				addNode(current.getRight(), newNode);
			} else {
				current.setRight(newNode);
				newNode.setParent(current);
			}
		} else {
			// the new node is > the current node go right

			if (current.getLeft() != null) {
				addNode(current.getLeft(), newNode);
			} else {
				current.setLeft(newNode);
				newNode.setParent(current);
			}
		}
	}

	/**
	 * start the add node process
	 * 
	 * @param data
	 */
	private void addNode(E data) {
		TreeNode<E> node = new TreeNode(data, head);

		if (head == null) {
			// set our new node to the top
			head = node;
			current = head;
		} else {
			// place the new node in the proper place

			if (comparator.compare(node.data, head.data) <= 0) {
				// the new node is < the current node go left or add
				if (head.getRight() != null) {
					addNode(head.getRight(), node);
				} else {
					head.setRight(node);
					node.setParent(head);
				}
			} else {
				// the new node is > the current node go right

				if (head.getLeft() != null) {
					addNode(head.getLeft(), node);
				} else {
					head.setLeft(node);
					node.setParent(head);
				}
			}

		}

	}

	/**
	 * Set the in order list
	 */
	private void getInOrder(TreeNode root) {

		// visit the left until there are none, hit the node, get the greater
		// node and repeat
		if (root != null) {
			// traverse to smallest connected and also to next smallest; etc.
			getInOrder(root.getLeft());

			// add the smallest unadded data to the array
			tosortlist.add((E) root.data);

			// get the next greatest and repeat the first iteration
			getInOrder(root.right);
		}

	}

	/**
	 * Travers the tree nodes 'In order'
	 * 
	 * @return
	 */
	private ArrayList<E> InOrderTraversal() {

		if (tosortlist == null) {
			tosortlist = new ArrayList<E>();
		}

		TreeNode root = head;

		getInOrder(root);

		return tosortlist;
	}

	/**
	 * Control the Sort
	 * 
	 * @return
	 */
	private void sort() {

		// create our heap and delete the old data in the process
		while (tosortlist.size() > 0) {
			addNode(tosortlist.get(0));
			tosortlist.remove(0);
		}

		// eliminate the old list to clear out memory
		tosortlist = null;
	}

	/**
	 * Get Reversed With Access to Tree
	 * 
	 * @param root
	 */
	private void getReversed(TreeNode root) {

		// get to greatest, then print greatest, then get next greatest by
		// getting connected left value; etc.
		if (root != null) {

			// get the greatest
			getReversed(root.getRight());

			// add the greatest unadded value
			tosortlist.add((E) root.getData());

			// get the next greatest
			getReversed(root.getLeft());
		}

	}

	/**
	 * Get ReversedArrayList
	 * 
	 * @return
	 */
	public ArrayList<E> getDescendingOrder() {

		TreeNode root = null;
		tosortlist = new ArrayList<E>();

		getReversed(root);

		return tosortlist;

	}

	/**
	 * Add in postorder
	 * 
	 * @param root
	 */
	private void getPostOrder(TreeNode root) {

		if (root != null) {

			// add the leftmost connected node
			getPostOrder(root.getLeft());

			// add the rightmost node
			getPostOrder(root.getRight());

			// add the unadded node
			tosortlist.add((E) root.getData());

		}

	}

	/**
	 * Get In Postorder. This could be useful for testing.
	 * 
	 * @return
	 */
	public ArrayList<E> getPostOrder(boolean delete) {
		TreeNode root = head;
		tosortlist = new ArrayList<E>();

		if (root != null) {
			getPostOrder(root);
		}

		if (delete) {
			head = null;
		}

		return tosortlist;
	}

	/**
	 * Get in pre order
	 * 
	 * @param node
	 */
	private void getPreOrder(TreeNode root) {

		if (root != null) {
			tosortlist.add((E) root.getData());
			getPreOrder(root.getLeft());
			getPreOrder(root.getRight());
		}

	}

	/**
	 * Get a Tree in pre-order to get a heap or a binary tree rep.
	 * 
	 * @return
	 */
	public ArrayList<E> getPreOrder() {

		TreeNode root = head;
		tosortlist = new ArrayList<E>();
		getPreOrder(root);
		return tosortlist;
	}

	/**
	 * Gets the BFS list. Iterate down to the level using the visited edges.
	 * Then back up be an increasing number of levels and then back down; etc.
	 * This is not dfs so the backtracking is based on a level of iteration
	 * where 2^(level) is the number of nodes to potentially visit (checking at
	 * each level suffices). Each Previous level is checked for branches.
	 * Termination is when no nodes are found for a certain traversal. I stored
	 * a parent but this can be done by iterating all the way down from the head
	 * (bleck!).
	 */
	private void itBreadthFirst() {
		TreeNode currentnode = head;
		int level = 0;
		int currentlevel = 0;

		// current node will be null when both left and right have been visited
		// and all are null
		while (currentnode != null) {
			// get left and get our order
			if (currentnode.getLeft().isVisited() == true) {
				currentnode = currentnode.getLeft();
			} else {
				// add the current level
			}

			// go right
		}

	}

	/**
	 * Gets a Breadth first Rep by storing level and iterating down to it 2^n
	 * nodes per level Speed was sacrificed for memory consumption incase the
	 * tree is huge | memory small
	 */
	public ArrayList<E> getBreadthFirst() {
		tosortlist = new ArrayList<E>();
		itBreadthFirst();
		return tosortlist;
	}

	/**
	 * Reset everything
	 * 
	 */
	public void reset() {
		head = null;
		tosortlist = null;
	}

	/**
	 * Run the sorting process
	 * 
	 * @return
	 */
	public void run() {
		head = null;
		sort();
	}

}
