package edu.buffalo.cse562.EquivalentFilters;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import edu.buffalo.cse562.RATree.Node;

public class BreakSelect implements Visitor{
	Expression equalToExpr = null;
	Expression compExpr = null;
	
	public void visit(Node parent, Node child, String link){
		Node nParent = parent;
		Node nChild = child;
		Expression condition = null;
		ArrayList<Object> operation =null;
		if(nChild==null)
			return;
		
		if(nChild.getOptName().equals("WHERE")){
			operation = (ArrayList<Object>) nChild.getValue();
			condition = (Expression) operation.get(0);
			BinaryExpression inputExpr = (BinaryExpression)condition;
			Expression tempExpr = null; 
			BinaryExpression resultExpr = null;
			ArrayList<Expression> exprList = null;
			ArrayList<Expression> tempExprList = null;
			Expression equalToExpr = null;
			Expression compExpr = null;
			Expression tempEqualToExpr = null;
			Expression tempCompExpr = null;
			
			if(inputExpr instanceof AndExpression){
				exprList = getAndComp(inputExpr);
				for(Expression expr : exprList){
					if(expr.toString().contains("=") && equalToExpr==null)
						equalToExpr = expr;
					else if(expr.toString().contains("=") && equalToExpr!=null)
						equalToExpr = new AndExpression(equalToExpr,expr);
					else if(!expr.toString().contains("=") && compExpr==null)
						compExpr =expr;
					else if(!expr.toString().contains("=") && compExpr!=null)
						compExpr = new AndExpression(compExpr, expr);
				}
			}
			
			if(inputExpr instanceof OrExpression){
				exprList = getOrComp(inputExpr);
			
			
				for(Expression expr : exprList){
					tempEqualToExpr = null;
					tempCompExpr = null;
				
					if(expr instanceof AndExpression){
						tempExprList = getAndComp(expr);
						for(Expression tExpr : tempExprList){
							if(tExpr.toString().contains("=") && tempEqualToExpr==null)
								tempEqualToExpr = tExpr;
							else if(tExpr.toString().contains("=") && tempEqualToExpr!=null)
								tempEqualToExpr = new AndExpression(tempEqualToExpr,tExpr);
							else if(!tExpr.toString().contains("=") && tempCompExpr==null)
								tempCompExpr =tExpr;
							else if(!tExpr.toString().contains("=") && tempCompExpr!=null)
								tempCompExpr = new AndExpression(tempCompExpr, tExpr);
						}
					}
					else if(expr.toString().contains("=") && equalToExpr==null)
						equalToExpr = expr;
					else if(expr.toString().contains("=") && equalToExpr!=null)
						equalToExpr = new OrExpression(equalToExpr,expr);
					else if(!expr.toString().contains("=") && compExpr==null)
						compExpr =expr;
					else if(!expr.toString().contains("=") && compExpr!=null)
						compExpr = new OrExpression(compExpr, expr);
				
					if(equalToExpr!=null && tempEqualToExpr!=null)
						equalToExpr = new OrExpression(tempEqualToExpr,equalToExpr);
					else if(equalToExpr==null && tempEqualToExpr!=null)
						equalToExpr = tempEqualToExpr;
					if(compExpr!=null && tempCompExpr!=null)
						compExpr = new OrExpression(tempCompExpr, compExpr);
					else if(compExpr==null && tempCompExpr!=null)
						compExpr = tempCompExpr;
				}
			}
			
			this.compExpr = compExpr;
			this.equalToExpr = equalToExpr;
			
			if(this.compExpr!=null && this.equalToExpr!=null)
				splitNode(parent, child,  link);
		}
	}
	
	public ArrayList<Expression> getAndComp(Expression inputExpr){
		if(inputExpr==null)
			return null;
	
		Expression expr = inputExpr;
		Expression tempExpr = null;
		ArrayList<Expression> exprList = new ArrayList<Expression>(); 
	
		while(expr!=null){
			if(expr.toString().toUpperCase().contains("AND")){
				
				if(((AndExpression)expr).getLeftExpression().toString().toUpperCase().contains("AND")){
					tempExpr = ((AndExpression)expr).getRightExpression();
					expr = (BinaryExpression) ((AndExpression)expr).getLeftExpression();
					exprList.add(tempExpr);
					//System.out.println("right "+ tempExpr);
				}
				else {
					tempExpr = ((AndExpression)expr).getRightExpression();
					exprList.add(tempExpr);
					//System.out.println("right "+ tempExpr);
					tempExpr = ((AndExpression)expr).getLeftExpression();
					exprList.add(tempExpr);
					//System.out.println("right "+ tempExpr);
					expr =null;
				}
			}
		}
		
		return exprList;
	}
	
	
	public ArrayList<Expression> getOrComp(Expression inputExpr){
		if(inputExpr==null)
			return null;
	
		Expression expr = inputExpr;
		Expression tempExpr = null;
		ArrayList<Expression> exprList = new ArrayList<Expression>(); 
	
		while(expr!=null){
			if(expr.toString().toUpperCase().contains("OR")){
				
				if(((OrExpression)expr).getLeftExpression().toString().toUpperCase().contains("OR")){
					tempExpr = ((OrExpression)expr).getRightExpression();
					expr = (BinaryExpression) ((OrExpression)expr).getLeftExpression();
					exprList.add(tempExpr);
					//System.out.println("right "+ tempExpr);
				}
				else {
					tempExpr = ((OrExpression)expr).getRightExpression();
					exprList.add(tempExpr);
					//System.out.println("right "+ tempExpr);
					tempExpr = ((OrExpression)expr).getLeftExpression();
					exprList.add(tempExpr);
					//System.out.println("right "+ tempExpr);
					expr =null;
				}
			}
		}
		
		return exprList;
	}
	
	public Expression getEqualToExpr(){
		return this.equalToExpr;
	}
	public Expression getCompToExpr(){
		return this.compExpr;
	}
	
	public void splitNode(Node parent, Node child, String link){
		ArrayList<Object> opt = null; new ArrayList<Object>();
		Node tempNode1 = new Node(child.getPriority());
		opt =  new ArrayList<Object>();
		
		opt.add(this.compExpr);
		tempNode1.setValue(opt, child.getOptName());
		
		Node tempNode2 = new Node(child.getPriority());
		opt =  new ArrayList<Object>();
		opt.add(this.equalToExpr);
		tempNode2.setValue(opt, child.getOptName());	
		tempNode1.setRight(tempNode2);
		tempNode2.setLeft(child.getLeft());
		tempNode2.setRight(child.getRight());
		
		if(link.equals("LEFT"))
			parent.setLeft(tempNode1);
		else
			parent.setRight(tempNode1);
		
	}
	
}
