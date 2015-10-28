package edu.buffalo.cse562.RATree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.select.SubSelect;

public class LoadOperators {

	HashMap<String, ArrayList<Object>> queryMap = null;

	public RATree load(
			HashMap<String, HashMap<String, ArrayList<Object>>> compMap) {

		String key = null;
		ArrayList<Object> component = null;
		Node opt = null;
		RATree tree = null;
		String aliasName = null;
		Node root = null;
		HashMap<String, Node> rootMap = new HashMap<String, Node>();
		HashMap<String, RATree> treeMap = new HashMap<String, RATree>();

		for (String stmt : compMap.keySet()) {

			tree = new RATree();

			key = "LIMIT";
			component = compMap.get(stmt).get(key);
			if (component != null) {
				if (tree.getRoot() == null && rootMap.get(stmt) != null) {
					root = rootMap.get(stmt);
					opt = root;
				} else if (tree.getRoot() == null && rootMap.get(stmt) == null) {
					opt = new Node(0);
					rootMap.put(stmt, opt);
				} else
					opt = new Node(0);

				opt.setValue(component, key);
				tree.loadTree(opt);
			}

			key = "DISTINCT";
			component = compMap.get(stmt).get(key);
			if (component != null) {
				if (tree.getRoot() == null && rootMap.get(stmt) != null) {
					root = rootMap.get(stmt);
					opt = root;
				} else if (tree.getRoot() == null && rootMap.get(stmt) == null) {
					opt = new Node(1);
					rootMap.put(stmt, opt);
				} else
					opt = new Node(1);

				opt.setValue(component, key);
				tree.loadTree(opt);
			}

			key = "SELECT";
			component = compMap.get(stmt).get(key);

			if (component != null && compMap.get(stmt).get("GROUPBY") == null) {
				if (tree.getRoot() == null && rootMap.get(stmt) != null) {
					root = rootMap.get(stmt);
					opt = root;
				} else if (tree.getRoot() == null && rootMap.get(stmt) == null) {
					opt = new Node(3);
					rootMap.put(stmt, opt);
				} else
					opt = new Node(3);

				opt.setValue(component, key);
				tree.loadTree(opt);
			}

			key = "ORDERBY";
			component = compMap.get(stmt).get(key);

			if (component != null) {
				if (tree.getRoot() == null && rootMap.get(stmt) != null) {
					root = rootMap.get(stmt);
					opt = root;
				} else if (tree.getRoot() == null && rootMap.get(stmt) == null) {
					opt = new Node(3);
					rootMap.put(stmt, opt);
				} else
					opt = new Node(3);

				opt.setValue(component, key);
				tree.loadTree(opt);
			}

			/*
			 * key = "SELECT"; component = compMap.get(stmt).get(key);
			 * 
			 * if(component!=null){ if(tree.getRoot()==null &&
			 * rootMap.get(stmt)!=null){ root = rootMap.get(stmt); opt = root; }
			 * else if(tree.getRoot()==null && rootMap.get(stmt)==null) { opt =
			 * new Node(3); rootMap.put(stmt, opt); } else opt = new Node(3);
			 * 
			 * opt.setValue(component, key); tree.loadTree(opt); }
			 */

			key = "HAVING";
			component = compMap.get(stmt).get(key);

			if (component != null) {
				if (tree.getRoot() == null && rootMap.get(stmt) != null) {
					root = rootMap.get(stmt);
					opt = root;
				} else if (tree.getRoot() == null && rootMap.get(stmt) == null) {
					opt = new Node(4);
					rootMap.put(stmt, opt);
				} else
					opt = new Node(4);

				opt.setValue(component, key);
				tree.loadTree(opt);
			}

			key = "GROUPBY";
			// component = compMap.get(stmt).get(key);
			component = compMap.get(stmt).get("SELECT");

			if (component != null && compMap.get(stmt).get(key) != null) {

				if (tree.getRoot() == null && rootMap.get(stmt) != null) {
					root = rootMap.get(stmt);
					opt = root;
				} else if (tree.getRoot() == null && rootMap.get(stmt) == null) {
					opt = new Node(5);
					rootMap.put(stmt, opt);
				} else
					opt = new Node(5);

				opt.setValue(component, key);
				component = compMap.get(stmt).get(key);
				opt.setsecOperation(component);
				tree.loadTree(opt);
			}

			key = "WHERE";
			component = compMap.get(stmt).get(key);

			if (component != null) {
				if (tree.getRoot() == null && rootMap.get(stmt) != null) {
					root = rootMap.get(stmt);
					opt = root;
				} else if (tree.getRoot() == null && rootMap.get(stmt) == null) {
					opt = new Node(6);
					rootMap.put(stmt, opt);
				} else
					opt = new Node(6);

				opt.setValue(component, key);
				tree.loadTree(opt);
			}

			key = "JOIN";
			aliasName = null;
			component = compMap.get(stmt).get(key);
			if (component != null) {
				for (Object comp : component) {
					if (tree.getRoot() == null && rootMap.get(stmt) != null) {
						root = rootMap.get(stmt);
						opt = root;
					} else if (tree.getRoot() == null
							&& rootMap.get(stmt) == null) {
						opt = new Node(7);
						rootMap.put(stmt, opt);
					} else
						opt = new Node(7);

					opt.setValue(null, key);
					tree.loadTree(opt);
				}
			}

			key = "FROM";
			aliasName = null;
			component = compMap.get(stmt).get(key);

			if (component != null) {
				for (Object comp : component) {
					if (comp instanceof SubSelect) {
						aliasName = ((SubSelect) comp).getAlias();
						if (aliasName != null) {
							root = rootMap.get(aliasName);
							if (root == null) {
								root = new Node(8);
								rootMap.put(aliasName, root);
								// System.out.println("Root priority 0:"+root.getPriority());
								tree.loadTree(root);
							} else {
								rootMap.put(aliasName, root);
								// System.out.println("Root priority 1:"+root.getPriority());
								int priority = root.getPriority();
								root.setPriority(8);
								tree.loadTree(root);
								root.setPriority(priority);
							}
						}
					} else {
						if (tree.getRoot() == null && rootMap.get(stmt) != null) {
							root = rootMap.get(stmt);
							opt = root;
						} else if (tree.getRoot() == null
								&& rootMap.get(stmt) == null) {
							opt = new Node(8);
							rootMap.put(stmt, opt);
						} else
							opt = new Node(8);
						// System.out.println("key: "+key+" component: "+comp);
						ArrayList<Object> newComp = new ArrayList<Object>();
						newComp.add(comp);
						opt.setValue(newComp, key);
						tree.loadTree(opt);
					}
				}
			}

			treeMap.put(stmt, tree);

		}
		tree = treeMap.get("S0");
		return tree;
	}
}
