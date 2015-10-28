package edu.buffalo.cse562.EquivalentFilters;

import java.util.ArrayList;
import java.util.function.Function;

import edu.buffalo.cse562.RATree.Node;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class MatchOperators {

public boolean checkType(ArrayList<Object> pCondition,String type)	{	
	ArrayList<Expression> exprList = null;
	ArrayList<Expression> tempExprList = null;
	ArrayList<Object> condition = pCondition;
	BinaryExpression inputExpr = null;
	
	if((Expression)condition.get(0) instanceof Parenthesis)
		inputExpr =(BinaryExpression) ((Parenthesis)(condition.get(0))).getExpression();
	else
		inputExpr = (BinaryExpression)condition.get(0);
	
	int flag = 0; 
	
	if(inputExpr instanceof AndExpression){
		exprList = getAndComp(inputExpr);
		for(Expression expr : exprList){
			if(type.equals("MINORTHAN") && expr instanceof MinorThan)
				flag =1;
			else if(type.equals("MINORTHANEQUALS") && expr instanceof MinorThanEquals)
				flag=1;
			else if(type.equals("EQUALSTO") && expr instanceof EqualsTo)
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
					if(type.equals("MINORTHAN") && tExpr instanceof MinorThan)
						flag =1;
					else if(type.equals("MINORTHANEQUALS") && tExpr instanceof MinorThanEquals)
						flag=1;
					else if(type.equals("EQUALSTO") && tExpr instanceof EqualsTo)
						flag =1;
				}
				if(flag==0)
					return false;
				}
			else{
				if(type.equals("MINORTHAN") && expr instanceof MinorThan)
					flag =1;
				else if(type.equals("MINORTHANEQUALS") && expr instanceof MinorThanEquals)
					flag=1;
				else if(type.equals("EQUALSTO") && expr instanceof EqualsTo)
					flag =1;
				else
					return false;
			}
		}
	}
	else{
		Expression tempExp = (Expression) condition.get(0);
		if(tempExp instanceof Parenthesis)
			tempExp = ((Parenthesis)tempExp).getExpression();
		 
		
		if(type.equals("MINORTHAN") && tempExp instanceof MinorThan)
			return true;
		else if(type.equals("MINORTHANEQUALS") && tempExp instanceof MinorThanEquals)
			return true;
		else if(type.equals("EQUALSTO") && tempExp instanceof EqualsTo)
			return true;
		else
			return false;
	}
	
	return true;	
}

public boolean checkType(Expression pCondition,String type)	{	
	ArrayList<Expression> exprList = null;
	ArrayList<Expression> tempExprList = null;
	Expression condition = pCondition;
	BinaryExpression inputExpr = null;
	
	if((Expression)condition instanceof Parenthesis)
		inputExpr =(BinaryExpression) ((Parenthesis)(condition)).getExpression();
	else
		inputExpr = (BinaryExpression)condition;
	
	int flag = 0; 
	
	if(inputExpr instanceof AndExpression){
		exprList = getAndComp(inputExpr);
		for(Expression expr : exprList){
			if(type.equals("MINORTHAN") && expr instanceof MinorThan)
				flag =1;
			else if(type.equals("MINORTHANEQUALS") && expr instanceof MinorThanEquals)
				flag=1;
			else if(type.equals("EQUALSTO") && expr instanceof EqualsTo)
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
					if(type.equals("MINORTHAN") && tExpr instanceof MinorThan)
						flag =1;
					else if(type.equals("MINORTHANEQUALS") && tExpr instanceof MinorThanEquals)
						flag=1;
					else if(type.equals("EQUALSTO") && tExpr instanceof EqualsTo)
						flag =1;
				}
				if(flag==0)
					return false;
				}
			else{
				if(type.equals("MINORTHAN") && expr instanceof MinorThan)
					flag =1;
				else if(type.equals("MINORTHANEQUALS") && expr instanceof MinorThanEquals)
					flag=1;
				else if(type.equals("EQUALSTO") && expr instanceof EqualsTo)
					flag =1;
				else
					return false;
			}
		}
	}
	else{
		Expression tempExp = (Expression) condition;
		if(tempExp instanceof Parenthesis)
			tempExp = ((Parenthesis)tempExp).getExpression();
		 
		
		if(type.equals("MINORTHAN") && tempExp instanceof MinorThan)
			return true;
		else if(type.equals("MINORTHANEQUALS") && tempExp instanceof MinorThanEquals)
			return true;
		else if(type.equals("EQUALSTO") && tempExp instanceof EqualsTo)
			return true;
		else
			return false;
	}
	
	return true;	
}



public boolean checkCondition(Node parent,Node child, char equiJoinInd){
		
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
				flag=0;
				if(checkComp(expr,child,equiJoinInd)){
					flag =1;
				}
				else
					break;
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
							flag =0;
							if(checkComp(tExpr,child,equiJoinInd))
								flag = 1;
							else
								break;
					}
					if(flag==0)
						return false;
					}
				else{
					if(checkComp(expr,child,equiJoinInd))
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
			 
			
			if(checkComp(tempExp,child,equiJoinInd))
				return true;
			else
				return false;
		}
		
		return true;
	}
public boolean matchCondition(Expression condition,Node child, char equiJoinInd){
	
	if(condition==null)
		return false;
	if(child==null)
		return false;
	
	ArrayList<Expression> exprList = null;
	ArrayList<Expression> tempExprList = null;
	
	BinaryExpression inputExpr = null;
	
	if((Expression)condition instanceof Parenthesis)
		inputExpr =(BinaryExpression) ((Parenthesis)(condition)).getExpression();
	else
		inputExpr = (BinaryExpression)condition;
	
	int flag = 0; 
	
	if(inputExpr instanceof AndExpression){
		exprList = getAndComp(inputExpr);
		for(Expression expr : exprList){
			flag=0;
			if(checkComp(expr,child,equiJoinInd)){
				flag =1;
			}
			else
				break;
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
						flag =0;
						if(checkComp(tExpr,child,equiJoinInd))
							flag = 1;
						else
							break;
				}
				if(flag==0)
					return false;
				}
			else{
				if(checkComp(expr,child,equiJoinInd))
					flag = 1;
				else
					return false;
			}
		}
	}
	else{
		Expression tempExp = condition;
		if(tempExp instanceof Parenthesis)
			tempExp = ((Parenthesis)tempExp).getExpression(); 
		
		if(checkComp(tempExp,child,equiJoinInd))
			return true;
		else
			return false;
	}
	
	return true;
}

public boolean checkComp(Expression expr,Node n,char equiJoinInd){
	Expression inputExpr = expr;
	Expression leftExpr = null;
	Expression rightExpr = null;
	Expression nodeExpr = null;
	ArrayList<ColumnDefinition> schema = n.getSchema();
	String left = null;
	String right =null;
	int rflag = 0;
	int lflag = 0;
	String rAlias = null;
	String lAlias = null;
	String alias = null;
	String rChildAlias = null;
	String lChildAlias = null;
	/*if(n.getValue() == null)
		return true;*/
	
	/*Extraction of left and right expression from Binary expression*/ 
	if(inputExpr!=null){
		if(inputExpr instanceof MinorThan){
			leftExpr = ((MinorThan)expr).getLeftExpression();
			rightExpr = ((MinorThan)expr).getRightExpression();
		}
		else if(inputExpr instanceof MinorThanEquals){
			leftExpr = ((MinorThan)expr).getLeftExpression();
			rightExpr = ((MinorThan)expr).getRightExpression();
		}
		else if(inputExpr instanceof EqualsTo){
			leftExpr = ((EqualsTo)expr).getLeftExpression();
			rightExpr = ((EqualsTo)expr).getRightExpression();
		}
		else if(inputExpr instanceof GreaterThan){
			leftExpr = ((GreaterThan)expr).getLeftExpression();
			rightExpr = ((GreaterThan)expr).getRightExpression();
		}
		else if(inputExpr instanceof GreaterThanEquals){
			leftExpr = ((GreaterThanEquals)expr).getLeftExpression();
			rightExpr = ((GreaterThanEquals)expr).getRightExpression();
		}
		else if(inputExpr instanceof Parenthesis){
			if(matchCondition(((Parenthesis) inputExpr).getExpression(),n,equiJoinInd))
					return true;
			else
				return false;
		}
		
	}
	
	/*Extract table name or alias name from node expression*/
	if(n.getValue()!=null){
		if(((ArrayList<Object>)(n.getValue())).get(0) instanceof Table){
			alias =  ((Table)((ArrayList<Object>)(n.getValue())).get(0)).getAlias();
			if(alias == null)
				alias = ((Table)((ArrayList<Object>)(n.getValue())).get(0)).getName().toUpperCase();
		}
		else if(((ArrayList<Object>)(n.getValue())).get(0) instanceof Expression){	
			nodeExpr =  (Expression) ((ArrayList<Object>)(n.getValue())).get(0);
			if(nodeExpr instanceof Parenthesis)
				alias = ((Parenthesis)nodeExpr).getExpression().toString().toUpperCase();
			else
				alias = nodeExpr.toString();
		}
	}
	else if(n.getLeft().getValue()!=null && n.getRight().getValue()!=null){
		Object lchildExp = ((ArrayList<Object>)(n.getLeft().getValue())).get(0);
		Object rchildExp = ((ArrayList<Object>)(n.getRight().getValue())).get(0);
		
		if(lchildExp instanceof Table){
			lChildAlias =  ((Table)(lchildExp)).getAlias();
			if(lChildAlias == null)
				lChildAlias = ((Table)lchildExp).getName().toUpperCase();
		}
		else if(lchildExp instanceof Expression){	
			nodeExpr =  (Expression) lchildExp;
			if(nodeExpr instanceof Parenthesis)
				lChildAlias = ((Parenthesis)nodeExpr).getExpression().toString().toUpperCase();
			else
				lChildAlias = nodeExpr.toString();
		}
		
		if(rchildExp instanceof Table){
			rChildAlias =  ((Table)(rchildExp)).getAlias();
			if(rChildAlias == null)
				rChildAlias = ((Table)rchildExp).getName().toUpperCase();
		}
		else if(rchildExp instanceof Expression){	
			nodeExpr =  (Expression) rchildExp;
			if(nodeExpr instanceof Parenthesis)
				rChildAlias = ((Parenthesis)nodeExpr).getExpression().toString().toUpperCase();
			else
				rChildAlias = nodeExpr.toString();
		}
	}
	/*Extract table name or alias name from Left expression of binary expression*/
	if(leftExpr.toString().contains(".")){
		int index = leftExpr.toString().indexOf(".");
		int length = leftExpr.toString().length();
		left = leftExpr.toString().substring(index+1, length).toUpperCase();
		lAlias = leftExpr.toString().substring(0,index).toUpperCase();
	}
	else
		left = leftExpr.toString().toUpperCase();

	/*Extract table name or alias name from right expression of binary expression*/
	if(rightExpr.toString().contains(".")){
		int index = rightExpr.toString().indexOf(".");
		int length = rightExpr.toString().length();
		right = rightExpr.toString().substring(index+1, length).toUpperCase();
		rAlias = rightExpr.toString().substring(0,index).toUpperCase();
	}
	else
		right =  rightExpr.toString().toUpperCase();
		
	/*Matching table name in case of FROM Operator node*/
	if(alias!=null){
		if(rAlias!=null){
			if(!rAlias.equals(alias))
				return false;
		}
		
		if(lAlias!=null){
			if(!lAlias.equals(alias))
				return false;
		}
	}
	else{
		if(rAlias!=null && rChildAlias!=null && lChildAlias!=null){
			if(!rAlias.equals(rChildAlias) && !rAlias.equals(lChildAlias))
				return false;
		}
		if(lAlias!=null && rChildAlias!=null && lChildAlias!=null){
			if(!lAlias.equals(rChildAlias) && !lAlias.equals(lChildAlias))
				return false;
		}
	}
	/*Matching expression with columns*/
	if(leftExpr instanceof net.sf.jsqlparser.expression.Function &&
			equiJoinInd=='N')
		lflag=1;
	else if((leftExpr instanceof StringValue || leftExpr instanceof DateValue || leftExpr instanceof DoubleValue)  &&
			equiJoinInd=='N')
		lflag = 1;
	else{
		for(ColumnDefinition col: schema){
			if(left.equals(col.getColumnName().toUpperCase()) 
					|| leftExpr.toString().toUpperCase().equals(col.getColumnName().toUpperCase())){
				lflag = 1;
				break;
			}
			
		}
	}
	
	/*Matching expression with columns*/
	if(rightExpr instanceof net.sf.jsqlparser.expression.Function &&
			equiJoinInd=='N')
		rflag = 1;
	else if((rightExpr instanceof StringValue || rightExpr instanceof DateValue || rightExpr instanceof DoubleValue)  &&
			equiJoinInd=='N')
		rflag = 1;
	else{
		for(ColumnDefinition col: schema){
			if(right.equals(col.getColumnName().toUpperCase()) 
					|| rightExpr.toString().toUpperCase().equals(col.getColumnName().toUpperCase())){
				rflag = 1;
				break;
			}
	}
	}
	
	if(lflag==1 && rflag==1)
		return true;
	else
		return false;
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
				
			}
			else {
				tempExpr = ((AndExpression)expr).getRightExpression();
				exprList.add(tempExpr);
				
				tempExpr = ((AndExpression)expr).getLeftExpression();
				exprList.add(tempExpr);
				
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
	public boolean checkEquiJoin(Expression condition){
		ArrayList<Expression> exprList = null;
		BinaryExpression inputExpr = null;
		Expression leftOpt = null;
		Expression rightOpt = null;
		
		if((Expression)condition instanceof Parenthesis)
			inputExpr =(BinaryExpression) ((Parenthesis)(condition)).getExpression();
		else
			inputExpr = (BinaryExpression)condition;
		
		int flag = 0;
		
		if(inputExpr instanceof AndExpression)
			exprList = getAndComp(inputExpr);
		else if(inputExpr instanceof OrExpression)
			exprList = getOrComp(inputExpr);
		else {
			leftOpt = inputExpr.getLeftExpression();
			rightOpt = inputExpr.getRightExpression();
			
			if(leftOpt instanceof net.sf.jsqlparser.expression.Function 
					|| leftOpt instanceof DateValue ||leftOpt instanceof StringValue
					||leftOpt instanceof DoubleValue){
				return false;
			}
			if(rightOpt instanceof net.sf.jsqlparser.expression.Function 
					|| rightOpt instanceof DateValue ||rightOpt instanceof StringValue
					||rightOpt instanceof DoubleValue){
				return false;
			}
			
			return true;
		}
		
		if(exprList!=null){
			for(Expression expr : exprList){
				if(expr instanceof BinaryExpression){
					leftOpt = ((BinaryExpression)expr).getLeftExpression();
					rightOpt = ((BinaryExpression)expr).getRightExpression();
					if(leftOpt instanceof net.sf.jsqlparser.expression.Function 
							|| leftOpt instanceof DateValue ||leftOpt instanceof StringValue
							||leftOpt instanceof DoubleValue){
						return false;
					}
					if(rightOpt instanceof net.sf.jsqlparser.expression.Function 
							|| rightOpt instanceof DateValue ||rightOpt instanceof StringValue
							||rightOpt instanceof DoubleValue){
						return false;
					}
				}
			}
		}	
			return true;
	}
}
