package edu.buffalo.cse562.EquivalentFilters;

import edu.buffalo.cse562.RATree.Node;

public interface Visitor {
	public void visit(Node parent, Node child, String link);
}
