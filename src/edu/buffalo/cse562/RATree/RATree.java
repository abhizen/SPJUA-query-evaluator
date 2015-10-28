package edu.buffalo.cse562.RATree;

import java.util.ArrayDeque;

import edu.buffalo.cse562.EquivalentFilters.Visitor;

public class RATree  {
	private Node root;
	
	public RATree(){
		
	}
	
	public Node getRoot(){
		return root;
	}
	
	/*public void loadTree(Node n){
		Node y = null;
		Node x = null;
		
		if(n==null)
			return;
		
		//System.out.println(n.getOptName());
		
		if(this.root==null){
			this.root = n;
			return;
		}
		
		x = this.root;
		
		while(x!=null)
		{
			y = x;
				if(x.getRight()!=null && x.getLeft()==null && 
							 x.getOptName().equals("JOIN")){
					x = x.getLeft();
				}
				else
					x = x.getRight();
		}
		
		if(y.getRight()!=null && y.getLeft()==null && 
				 y.getOptName().equals("JOIN"))
			y.setLeft(n);
    	else
    		y.setRight(n);
	}*/
	
	public void trav(Node n, Visitor filter){
		
		if(n==null)
			return;
		
		trav(n.getLeft(),  filter);
		
		trav(n.getRight(),  filter);
		
		filter.visit(n, n.getLeft(), "LEFT");
		filter.visit(n, n.getRight(), "RIGHT");
		//System.out.println(n.getOptName() +"  :" + n.getValue());
	}
	
	public Node findNode(Node n){
		Node left = null;
		Node right = null;
		if(n==null)
			return null;
		
		if(n.getOptName().equals("JOIN") && (n.getRight()==null||n.getLeft()==null))
			return n;
		else if(!n.getOptName().equals("JOIN") && n.getRight()==null)
			return n;
		
		left = findNode(n.getLeft());
		right = findNode(n.getRight());
		
		if(left!=null && right!=null){
			if(left.getOptName().equals("JOIN") && !right.getOptName().equals("JOIN"))
				return left;
			else if(!left.getOptName().equals("JOIN") && right.getOptName().equals("JOIN"))
				return right;
			else return left;
		}
		else if(left!=null) return left;
		else if(right!=null) return right;
		else return null;
	}
	public Node newFindNode(Node n){
		Node left = null;
		Node right = null;
		if(n==null)
			return null;
		
		if(n.getOptName().equals("FROM"))
			return null;
		else if(n.getOptName().equals("JOIN") && (n.getLeft()==null && n.getRight()==null))
			return n;
		else if(!n.getOptName().equals("JOIN") && !n.getOptName().equals("FROM") 
				&& n.getRight()==null)
			return n;
		
		left = newFindNode(n.getLeft());
		right = newFindNode(n.getRight());
		
		if(left!=null && right!=null){
			if(left.getOptName().equals("JOIN") && !right.getOptName().equals("JOIN"))
				return left;
			else if(!left.getOptName().equals("JOIN") && right.getOptName().equals("JOIN"))
				return right;
			else return left;
		}
		else if(left!=null) return left;
		else if(right!=null) return right;
		else if(left==null  && n.getLeft()!=null && n.getRight()==null )
			return n;
		else return null;
	}
	public void loadTree(Node n){
		Node x = null;
		
		
		if(n==null)
			return;
		
		if(this.root==null){
			this.root = n;
			return;
		}
		
		x = newFindNode(this.root);
		
		if(x.getOptName().equals("JOIN")){
			if(x.getLeft()==null)
				x.setLeft(n);
			else
				x.setRight(n);
		}
		else{
			x.setRight(n);
		}
		
	}
}
