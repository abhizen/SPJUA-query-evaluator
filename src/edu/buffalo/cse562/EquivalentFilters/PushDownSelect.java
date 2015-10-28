package edu.buffalo.cse562.EquivalentFilters;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.RATree.Node;

public class PushDownSelect implements Visitor{
	MatchOperators matchOpt = null;
	
	public PushDownSelect(){
		matchOpt = new MatchOperators();
	}
	
	public void visit(Node parent, Node child, String link){
		Node nParent = parent;
		Node nChild = child;
		Node newParent = null;
		Expression condition = null;
		ArrayList<Object> operation =null;
		if(nChild==null)
			return;
		
		
		if(nChild.getOptName().equals("WHERE")){
			operation = (ArrayList<Object>) nChild.getValue();
			condition = (Expression) operation.get(0);
			newParent = nChild.getRight();
			
			if(newParent!=null && !newParent.getOptName().equals("FROM")
					&& !newParent.isSet()) {
					 /*&& (matchOpt.checkType(operation, "MINORTHAN")
							||matchOpt.checkType(operation, "MINORTHANEQUALS"))){*/
			
				/*if(newParent.getLeft()!=null) && 
						checkSchema(newParent.getLeft().getSchema(),
								nChild.getSchema()))*/{
					Node temp = null;
			
					if(newParent.getLeft()!=null &&
							matchOpt.checkCondition(child,newParent.getLeft(),'N')){
						/*if(newParent.getRight()!=null && checkSchema(newParent.getRight().getSchema(),
								nChild.getSchema()) && matchOpt.checkCondition(child,newParent.getRight(),'N'))
							temp = getDuplicate(child);
						else*/
							temp = child;
							pushDown(parent,temp,child.getRight(),link,"LEFT");
			
					}
				else if(newParent.getRight()!=null  &&
						matchOpt.checkCondition(child,newParent.getRight(),'N')){ /*&& checkSchema(newParent.getRight().getSchema(),
						nChild.getSchema())){*/
			
					/*if(matchOpt.checkCondition(child,newParent.getRight(),'N')){*/
						child.setLock();
						pushDown(parent,child,child.getRight(),link,"RIGHT");
					
				}
				else{
					child.resetLock();
				}
			}
		}
				
		}
	}
	
	public boolean checkSchema(ArrayList<ColumnDefinition> parentSchema, 
			ArrayList<ColumnDefinition> childSchema){
		int flag = 0;
		
		if(parentSchema==null)
			return false;
		
		if(childSchema==null)
			return false;
		
		for(ColumnDefinition parentColDef : parentSchema){
			flag = 0;
			for(ColumnDefinition childColDef : childSchema){
				if(parentColDef.getColumnName().toUpperCase()
						.equals(childColDef.getColumnName().toUpperCase())){
					flag =1;
					break;
				}
			}
			
			if(flag==0)
				return false;
		}
		
		for(ColumnDefinition childColDef :  childSchema){
			flag = 0;
			for(ColumnDefinition parentColDef : parentSchema){
				if(parentColDef.getColumnName().toUpperCase()
						.equals(childColDef.getColumnName().toUpperCase())){
					flag =1;
					break;
				}
			}
			
			if(flag==0)
				return false;
		}
		
		
		return true;
	}
	
	public boolean checkCondition(Node parent,Node child){
		
		if(parent==null)
			return false;
		if(child==null)
			return false;
		
		ArrayList<Expression> exprList = null;
		ArrayList<Expression> tempExprList = null;
		ArrayList<Object> condition = (ArrayList<Object>) parent.getValue();
		BinaryExpression inputExpr = null;
		
		if((Expression)condition.get(0) instanceof Parenthesis)
			inputExpr =(BinaryExpression) ((Parenthesis)(condition.get(0))).getExpression();
		else
			inputExpr = (BinaryExpression)condition.get(0);
		
		int flag = 0; 
		
		if(inputExpr instanceof AndExpression){
			exprList = getAndComp(inputExpr);
			for(Expression expr : exprList){
				if(checkComp(expr,child))
					flag =1;
			}
			if(flag == 0)
				return false;
		}
		else if(inputExpr instanceof OrExpression){
			exprList = getOrComp(inputExpr);
		
			for(Expression expr : exprList){
				flag = 0;
				if(expr instanceof AndExpression){
					tempExprList = getAndComp(expr);
					
					for(Expression tExpr : tempExprList){
							if(checkComp(tExpr,child))
								flag = 1;
					}
					if(flag==0)
						return false;
					}
				else{
					if(checkComp(expr,child))
						flag = 1;
					else
						return false;
				}
			}
		}
		else{
			Expression tempExp = (Expression) condition.get(0);
			if(tempExp instanceof Parenthesis)
				tempExp = ((Parenthesis)tempExp).getExpression();
			 
			
			if(checkComp(tempExp,child))
				return true;
			else
				return false;
		}
		
		return true;
	}
	
	public Node getDuplicate(Node n){
		Node temp = new Node(n.getPriority());
		temp.setLeft(n.getLeft());
		temp.setRight(n.getRight());
		temp.setSchema(n.getSchema());
		temp.setsecOperation(n.getsecOperation());
		temp.setValue((ArrayList<Object>)n.getValue(), n.getOptName());
		
		return temp;
	}
	
	public void pushDown(Node parent,Node child,Node newParent, String link,String newLink){
		Node temp = child;
		if(link.toUpperCase().equals("LEFT")){
			parent.setLeft(child.getRight());
		}
		else
			parent.setRight(child.getRight());
			
		if(newLink.toUpperCase().equals("LEFT")){
			child.setRight(newParent.getLeft());
			newParent.setLeft(child);
		}
		else{
			child.setRight(newParent.getRight());
			newParent.setRight(child);
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

	public boolean checkComp(Expression expr,Node n){
		Expression leftExpr = ((MinorThan)expr).getLeftExpression();
		Expression rightExpr = ((MinorThan)expr).getRightExpression();
		Expression nodeExpr = null;
		ArrayList<ColumnDefinition> schema = n.getSchema();
		String left = null;
		String right =null;
		int rflag = 0;
		int lflag = 0;
		String rAlias = null;
		String lAlias = null;
		String alias = null;
		
	
		
		if(n.getValue() == null)
			return true;
		
		
		if(((ArrayList<Object>)(n.getValue())).get(0) instanceof Table){
			alias =  ((Table)((ArrayList<Object>)(n.getValue())).get(0)).getAlias();
			if(alias == null)
				alias = ((Table)((ArrayList<Object>)(n.getValue())).get(0)).getName().toUpperCase();
		}
		else if(((ArrayList<Object>)(n.getValue())).get(0) instanceof Expression){	
			nodeExpr =  (Expression) ((ArrayList<Object>)(n.getValue())).get(0);
			if(nodeExpr instanceof Parenthesis)
				alias = ((Parenthesis)nodeExpr).getExpression().toString().toUpperCase(); 
		}
		
		if(leftExpr.toString().contains(".")){
			int index = leftExpr.toString().indexOf(".");
			int length = leftExpr.toString().length();
			left = leftExpr.toString().substring(index+1, length);
			lAlias = leftExpr.toString().substring(0,index);
		}
		else
			left = leftExpr.toString();
		
		if(rightExpr.toString().contains(".")){
			int index = leftExpr.toString().indexOf(".");
			int length = leftExpr.toString().length();
			right = rightExpr.toString().substring(index+1, length);
			rAlias = rightExpr.toString().substring(0,index);
		}
		else
			right =  rightExpr.toString();
			
		//Matching table name
		if(lAlias!=null && rAlias!=null && (!lAlias.toUpperCase().equals(alias)||
				!rAlias.toUpperCase().equals(alias)) ){
			return false;
		}
		
		for(ColumnDefinition col: schema){
			if(left.toUpperCase().equals(col.getColumnName()) 
					|| leftExpr.toString().toUpperCase().equals(col.getColumnName()))
				lflag = 1;
		}
		
		for(ColumnDefinition col: schema){
			if(right.toUpperCase().equals(col.getColumnName()) 
					|| rightExpr.toString().toUpperCase().equals(col.getColumnName()))
				rflag = 1;
		}
		
		if(lflag==1 && rflag==1)
			return true;
		else
			return false;
	}
}
