package edu.buffalo.cse562.EquivalentFilters;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import edu.buffalo.cse562.RATree.Node;

public class TransformJoin implements Visitor{
MatchOperators mtchOpt = new MatchOperators(); 	

	public void visit(Node parent, Node child, String link){
		Node nParent = parent;
		Node nChild = child;
		Node nJoin = null;
		Node newParent = null;
		Expression condition = null;
		ArrayList<Object> operation =null;
		BinaryExpression inputExpr = null;
		ArrayList<Expression> exprList = null;
		
		if(nChild==null)
			return;
		
		
		if(nChild.getOptName().equals("WHERE")){
			operation = (ArrayList<Object>) nChild.getValue();
			condition = (Expression) operation.get(0);
			
			//check if operator is minorThan
			/*if(condition instanceof EqualsTo || (condition instanceof Parenthesis &&
					((Parenthesis)condition).getExpression() instanceof EqualsTo) || mtchOpt.checkType(condition, "EQUALSTO")
					&& mtchOpt.matchCondition(condition, nChild.getRight(),'Y')){*/
			if(mtchOpt.checkType(condition, "EQUALSTO")
					&& mtchOpt.checkEquiJoin(condition)){
			
				//check if child operator is JOIN
				if(nChild.getRight().getOptName().equals("JOIN")){
					
					//combine the two operator to enhanced join
					nJoin = nChild.getRight();
					
					nJoin.setValue((ArrayList<Object>) nChild.getValue(), "ENHANCEDJOIN");
					
					//remove WHERE operator node
					if(link.equals("LEFT"))
						parent.setLeft(nJoin);
					else
						parent.setRight(nJoin);
					
				}
				else
					return;
				
				
			}
			else
				return;
			
			
			
		}
	}
}
