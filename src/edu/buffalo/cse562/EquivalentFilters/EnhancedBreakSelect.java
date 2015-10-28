package edu.buffalo.cse562.EquivalentFilters;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import edu.buffalo.cse562.RATree.Node;

public class EnhancedBreakSelect implements Visitor{
	Expression equalToExpr = null;
	Expression compExpr = null;
	MatchOperators mtchOpt = null;
	
	public EnhancedBreakSelect(){
		mtchOpt = new MatchOperators();
	}
	
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
			BinaryExpression inputExpr = null;//(BinaryExpression)condition;
			Expression tempExpr = null; 
			BinaryExpression resultExpr = null;
			ArrayList<Expression> exprList = null;
			ArrayList<Expression> tempExprList = null;
			Expression equalToExpr = null;
			Expression compExpr = null;
			Expression leftPushableExpr = null;
			Expression rightPushableExpr = null;
			Expression tempEqualToExpr = null;
			Expression tempLeftPushableExpr = null;
			Expression tempRightPushableExpr = null;
			Expression tempCompExpr = null;
			
			if(condition instanceof Parenthesis)
				inputExpr = (BinaryExpression) ((Parenthesis)condition).getExpression();
			else
				inputExpr = (BinaryExpression)condition;
			
			if(inputExpr instanceof AndExpression){
				exprList = getAndComp(inputExpr);
				for(Expression expr : exprList){
					//System.out.println(expr);
					/*if(mtchOpt.checkType(expr,"EQUALSTO") 
							&& mtchOpt.matchCondition(expr, nChild.getRight(),'Y')){
						if(equalToExpr==null)
							equalToExpr = expr;
						else if(equalToExpr!=null )
							equalToExpr = new AndExpression(equalToExpr,expr);
					}
					else {*/
						if(nChild.getRight()!=null 
								&& mtchOpt.matchCondition(expr, nChild.getRight().getLeft(),'N')){
								if(leftPushableExpr==null){
									leftPushableExpr = expr;
								}
								else{
									leftPushableExpr =  new AndExpression(leftPushableExpr,expr);
								}
						}
						else if(nChild.getRight()!=null 
								&& mtchOpt.matchCondition(expr, nChild.getRight().getRight(),'N')){
								if(rightPushableExpr==null){
									rightPushableExpr = expr;
								}
								else{
									rightPushableExpr =  new AndExpression(rightPushableExpr,expr);
								}
						}
						else if(mtchOpt.checkType(expr,"EQUALSTO") 
								&& mtchOpt.matchCondition(expr, nChild.getRight(),'Y')){
							if(equalToExpr==null)
								equalToExpr = expr;
							else if(equalToExpr!=null )
								equalToExpr = new AndExpression(equalToExpr,expr);
						}
						else{
							if(compExpr==null)
								compExpr =expr;
							else if(compExpr!=null)
								compExpr = new AndExpression(compExpr, expr);
						}
					//}
					
				}
			}
			
			if(inputExpr instanceof OrExpression){
				exprList = getOrComp(inputExpr);
			
			
				for(Expression expr : exprList){
					tempEqualToExpr = null;
					tempCompExpr = null;
					//System.out.println(expr);
					if(expr instanceof AndExpression){
						tempExprList = getAndComp(expr);
						for(Expression tExpr : tempExprList){
							if(mtchOpt.checkType(tExpr,"EQUALSTO") 
									&& mtchOpt.matchCondition(tExpr, nChild.getRight(),'Y')){
								if(tempEqualToExpr==null)
									tempEqualToExpr = tExpr;
								else if(tempEqualToExpr!=null)
									tempEqualToExpr = new AndExpression(tempEqualToExpr,tExpr);
							}
							else{
								if(nChild.getRight()!=null 
										&& mtchOpt.matchCondition(tExpr, nChild.getRight().getLeft(),'N')){
										if(tempLeftPushableExpr==null){
											tempLeftPushableExpr = tExpr;
										}
										else{
											tempLeftPushableExpr =  new AndExpression(tempLeftPushableExpr,tExpr);
										}
								}
								else if(nChild.getRight()!=null 
										&& mtchOpt.matchCondition(tExpr, nChild.getRight().getRight(),'N')){
										if(tempRightPushableExpr==null){
											tempRightPushableExpr = tExpr;
										}
										else{
											tempRightPushableExpr =  new AndExpression(tempRightPushableExpr,tExpr);
										}
								}
								else{
									if(tempCompExpr==null)
										tempCompExpr =tExpr;
									else if(tempCompExpr!=null)
										tempCompExpr = new AndExpression(tempCompExpr, tExpr);
								}
								/*if(tempCompExpr==null)
									tempCompExpr =tExpr;
								else if(tempCompExpr!=null)
									tempCompExpr = new AndExpression(tempCompExpr, tExpr);*/
								}
						}
						
					}
					else if(mtchOpt.checkType(expr,"EQUALSTO") 
							&& mtchOpt.matchCondition(expr, nChild.getRight(),'Y')){
						if( equalToExpr==null)
							equalToExpr = expr;
						else if(equalToExpr!=null)
							equalToExpr = new OrExpression(equalToExpr,expr);
					}
					else{
						if(nChild.getRight()!=null 
								&& mtchOpt.matchCondition(expr, nChild.getRight().getLeft(),'N')){
								if(leftPushableExpr==null){
									leftPushableExpr = expr;
								}
								else{
									leftPushableExpr =  new OrExpression(leftPushableExpr,expr);
								}
						}
						else if(nChild.getRight()!=null 
								&& mtchOpt.matchCondition(expr, nChild.getRight().getRight(),'N')){
								if(rightPushableExpr==null){
									rightPushableExpr = expr;
								}
								else{
									rightPushableExpr =  new OrExpression(rightPushableExpr,expr);
								}
						}
						else{
						if(compExpr==null)
							compExpr =expr;
						else
							compExpr = new OrExpression(compExpr, expr);
						}
					}
					
					if(equalToExpr!=null && tempEqualToExpr!=null)
						equalToExpr = new OrExpression(tempEqualToExpr,equalToExpr);
					else if(equalToExpr==null && tempEqualToExpr!=null)
						equalToExpr = tempEqualToExpr;
					
					if(rightPushableExpr!=null && tempRightPushableExpr!=null)
						rightPushableExpr = 
								new OrExpression(tempRightPushableExpr,rightPushableExpr);
					else if(rightPushableExpr==null && tempRightPushableExpr!=null)
						rightPushableExpr = tempRightPushableExpr;
					
					if(leftPushableExpr!=null && tempLeftPushableExpr!=null)
						leftPushableExpr = 
								new OrExpression(tempLeftPushableExpr,leftPushableExpr);
					else if(leftPushableExpr==null && tempLeftPushableExpr!=null)
						leftPushableExpr = tempLeftPushableExpr;
					
					if(compExpr!=null && tempCompExpr!=null)
						compExpr = new OrExpression(tempCompExpr, compExpr);
					else if(compExpr==null && tempCompExpr!=null)
						compExpr = tempCompExpr;
				}
			}
			
			/*this.compExpr = compExpr;
			this.equalToExpr = equalToExpr;*/
			
			/*if(this.compExpr!=null && this.equalToExpr!=null)
				splitNode(parent, child,  link);*/
			if(leftPushableExpr!=null && rightPushableExpr!=null && equalToExpr!=null){
				splitNode(parent, child,  link,equalToExpr,leftPushableExpr);
				
				if(link.equals("RIGHT"))
					splitNode(parent, parent.getRight(),  link,leftPushableExpr,rightPushableExpr);
				if(link.equals("LEFT"))
					splitNode(parent, parent.getLeft(),  link,leftPushableExpr,rightPushableExpr);
				
				if(compExpr!=null){
					if(link.equals("RIGHT"))
						splitNode(parent, parent.getRight(),  link,rightPushableExpr,compExpr);
					if(link.equals("LEFT"))
						splitNode(parent, parent.getLeft(),  link,rightPushableExpr,compExpr);
				}
				
			}else if(leftPushableExpr!=null &&  equalToExpr!=null){
				splitNode(parent, child,  link,equalToExpr,leftPushableExpr);
				
				if(compExpr!=null){
					if(link.equals("RIGHT"))
						splitNode(parent, parent.getRight(),  link,leftPushableExpr,compExpr);
					if(link.equals("LEFT"))
						splitNode(parent, parent.getLeft(),  link,leftPushableExpr,compExpr);
				}
				
			}else if(rightPushableExpr!=null &&  equalToExpr!=null){
				splitNode(parent, child,  link,equalToExpr,rightPushableExpr);
				
				if(compExpr!=null){
					if(link.equals("RIGHT"))
						splitNode(parent, parent.getRight(),  link,rightPushableExpr,compExpr);
					if(link.equals("LEFT"))
						splitNode(parent, parent.getLeft(),  link,rightPushableExpr,compExpr);
				}
				
			}
			else if(rightPushableExpr!=null &&  leftPushableExpr!=null){
				splitNode(parent, child,  link,rightPushableExpr,leftPushableExpr);
				
				if(compExpr!=null){
					if(link.equals("RIGHT"))
						splitNode(parent, parent.getRight(),  link,leftPushableExpr,compExpr);
					if(link.equals("LEFT"))
						splitNode(parent, parent.getLeft(),  link,leftPushableExpr,compExpr);
				}
				
			}
			else if(rightPushableExpr!=null && compExpr!=null){
				splitNode(parent, child,  link,rightPushableExpr,compExpr);
			}
			else if(leftPushableExpr!=null && compExpr!=null){
				splitNode(parent, child,  link,leftPushableExpr,compExpr);
			}
			else if(equalToExpr!=null && compExpr!=null){
				splitNode(parent, child,  link,equalToExpr,compExpr);
			}
			
			/*Expression otherExpr = null;
			if((leftPushableExpr!=null || rightPushableExpr!=null || equalToExpr!=null)
					&& compExpr!=null){
				if(link.equals("RIGHT")){
					otherExpr = (Expression)((ArrayList<Object>)(parent.getRight().getValue())).get(0);
					splitNode(parent, parent.getRight(),  link,otherExpr ,compExpr);
				}
				else{
					otherExpr = (Expression)((ArrayList<Object>)(parent.getLeft().getValue())).get(0);
					splitNode(parent, parent.getLeft(),  link,otherExpr,compExpr);
				}
			}*/
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
	public void splitNode(Node parent, Node child, String link, Expression equalToExpr,
			Expression compExpr){
		ArrayList<Object> opt = null; new ArrayList<Object>();
		Node tempNode1 = new Node(child.getPriority());
		opt =  new ArrayList<Object>();
		
		opt.add(compExpr);
		tempNode1.setValue(opt, child.getOptName());
		
		Node tempNode2 = new Node(child.getPriority());
		opt =  new ArrayList<Object>();
		opt.add(equalToExpr);
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
