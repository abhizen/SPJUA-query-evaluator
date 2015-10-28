package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.Eval;
import edu.buffalo.cse562.EquivalentFilters.MatchOperators;

public class HashJoinOperator extends Eval implements Operator {
	Operator optL = null;
	Operator optR = null;
	Expression joinCondition = null;

	HashMap<String, Integer> outputSchema = new HashMap<String, Integer>();
	HashMap<String, Integer> outputSchemaR = new HashMap<String, Integer>();
	HashMap<String, Integer> outputSchemaL = new HashMap<String, Integer>();

	String aliasL = null;
	String aliasR = null;
	HashMap<String, ArrayList<Expression[]>> leftTupleMap = null;
	MatchOperators mtchOpt = new MatchOperators();
	private HashMap<String, String> groupbyLeftItmList = null;
	private HashMap<String, String> groupbyRightItmList = null;
	private ArrayList<Expression[]> tupleList = null;
	Iterator tupleListItr = null;
	Expression[] inputTuple = null;
	HashMap<String, String> tupleColumnMap = null;
	HashMap<String, String> hashTupleColumnMap = null;
	ArrayList<String> rightItmList = null;
	int abhinit = 0;
	static int state = 0;
	
	public HashJoinOperator(Operator optL, Operator optR,
			ArrayList<Object> condition) {
		this.optL = optL;
		this.optR = optR;
		String news = null;
		int index = 0;
		joinCondition = (Expression) condition.get(0);

		if (optL instanceof BigDataScanOperator) {
			this.aliasL = ((BigDataScanOperator) optL).getAlias();

			if (aliasL != null) {
				this.outputSchemaL = ((BigDataScanOperator) optL)
						.getOutputSchema();
				for (String col : this.outputSchemaL.keySet()) {
					
					if(!col.contains("."))
						news = aliasL + "." + col;
					else
						news = col;
					
					if (outputSchemaL.get(news) == null) {
						index = outputSchemaL.get(col);
						this.outputSchema.put(news, index);
					}
					else{
						index = outputSchemaL.get(news);
						this.outputSchema.put(news, index);
					}
				}

			} else {
				this.outputSchemaL = optL.getOutputSchema();
			}
		} else {

			this.outputSchemaL = optL.getOutputSchema();
		}

		if (optR instanceof BigDataScanOperator) {
			this.aliasR = ((BigDataScanOperator) optR).getAlias();

			if (aliasR != null) {
				this.outputSchemaR = ((BigDataScanOperator) optR)
						.getOutputSchema();

				if (outputSchema.isEmpty())
					outputSchema.putAll(outputSchemaL);
				for (String col : this.outputSchemaR.keySet()) {
					
					if(!col.contains("."))
						news = aliasR + "." + col;
					else
						news = col;
					
					if (outputSchemaR.get(news) == null) {
						index = outputSchemaR.get(col)
								+ this.outputSchemaL.size();
						
						this.outputSchema.put(news, index);
					}
					else{
						index = outputSchemaR.get(news)
								+ this.outputSchemaL.size();
						this.outputSchema.put(news, index);
					}

				}
			} else {
				this.outputSchemaR = optR.getOutputSchema();
				if (outputSchema.isEmpty())
					outputSchema.putAll(outputSchemaL);
				for (String col : this.outputSchemaR.keySet()) {
					index = outputSchemaR.get(col) + this.outputSchemaL.size();
					this.outputSchema.put(col, index);
				}
			}

		} else {
			this.outputSchemaR = optR.getOutputSchema();
			if (outputSchema.isEmpty())
				outputSchema.putAll(outputSchemaL);
			for (String col : this.outputSchemaR.keySet()) {
				index = outputSchemaR.get(col) + this.outputSchemaL.size();
				this.outputSchema.put(col, index);
			}
		}

		// Extract key from Join condition
		groupbyLeftItmList = extractColumns(joinCondition, this.outputSchemaL);
		groupbyRightItmList = extractColumns(joinCondition, this.outputSchemaR);
		
		//Sorting left list acc to right list
		rightItmList = this.sortSchema(groupbyLeftItmList, groupbyRightItmList);
		
		/*
		 * Extract mapping b/w right operator schema columns and right column on
		 * Equijoin
		 */
		tupleColumnMap = getTupleMetaData(outputSchemaR,
				this.groupbyRightItmList);
		
		/*
		 * Extract mapping b/w left operator schema columns and left column on
		 * Equijoin
		 */
		

		// Groupby tuples by key columns
		if(!this.groupbyLeftItmList.isEmpty()){
			hashTupleColumnMap = this.getTupleMetaData(this.outputSchemaL,
					this.groupbyLeftItmList);
			leftTupleMap = groupByTuples(this.optL, this.groupbyLeftItmList,
					this.outputSchemaL);
		}
		else{
			hashTupleColumnMap = this.getTupleMetaData(this.outputSchemaR,
					 this.groupbyRightItmList);
			leftTupleMap = groupByTuples(this.optR, this.groupbyRightItmList,
					this.outputSchemaR);
			
		}
		
		/*if(state==4){
			System.out.println(this.groupbyLeftItmList);
			System.out.println(this.rightItmList);
			//System.out.println(leftTupleMap);
		}*/
		// rightTupleMap = groupByTuples(this.optR,this.groupbyLeftItmList);
		state++;
	}

	public HashMap<String, String> extractColumns(Expression condition,
			HashMap<String, Integer> schema) {
		ArrayList<Expression> exprList = null;
		ArrayList<Expression> tempExprList = null;
		Expression joinCondition = condition;
		HashMap<String, String> groupbyLeftItmList = new HashMap<String, String>();
		HashMap<String, String> tempItmList = null;
		Expression leftExpr = null;
		Expression rightExpr = null;

		if (joinCondition instanceof Parenthesis)
			joinCondition = ((Parenthesis) joinCondition).getExpression();

		if (joinCondition instanceof OrExpression)
			exprList = mtchOpt.getOrComp(joinCondition);
		else if (joinCondition instanceof AndExpression)
			exprList = mtchOpt.getAndComp(joinCondition);
		else {
			exprList = new ArrayList<Expression>();
			exprList.add(joinCondition);
		}

		for (Expression expr : exprList) {
			if (expr instanceof Parenthesis)
				expr = ((Parenthesis) expr).getExpression();
			if (expr instanceof AndExpression || expr instanceof OrExpression) {
				tempItmList = extractColumns(expr, schema);
				groupbyLeftItmList.putAll(tempItmList);
			} else if (expr instanceof BinaryExpression) {

				leftExpr = ((BinaryExpression) expr).getLeftExpression();
				rightExpr = ((BinaryExpression) expr).getRightExpression();
				String left = leftExpr.toString().toUpperCase();
				String right = rightExpr.toString().toUpperCase();
				String tempLeft = null;
				String tempRight = null;
				int index = 0;

				if (left.contains(".")) {
					index = left.indexOf(".");
					tempLeft = left.substring(index + 1, left.length());
				} else
					tempLeft = left;

				if (right.contains(".")) {
					index = right.indexOf(".");
					tempRight = right.substring(index + 1, right.length());
				} else
					tempRight = right;

				/* requires change as schema */
				for (String col : schema.keySet()) {

					if (col.toUpperCase().equals(left)
							|| (col.contains(".") && !left.contains(".") &&
									col.toUpperCase().equals(tempLeft))) {
						
						if (groupbyLeftItmList.get(left) == null
								&& groupbyLeftItmList.get(tempLeft) == null)
							groupbyLeftItmList.put(left, col);
						
					} else if (col.toUpperCase().equals(right)
							|| (col.contains(".") && !right.contains(".") && 
									col.toUpperCase().equals(tempRight))) {
						
						if (groupbyLeftItmList.get(right) == null
								&& groupbyLeftItmList.get(tempRight) == null)
							groupbyLeftItmList.put(right, col);
						
					}

				}
			}
		}
		return groupbyLeftItmList;
	}

	@Override
	public HashMap<String, Integer> getOutputSchema() {
		return this.outputSchema;
	}

	@Override
	public Expression[] getTuple() {
		Expression[] tuple = null;
		Expression[] tempTuple = null;
		Expression[] resultTuple = null;

		Iterator colomnListItr = null;

		String col = null;
		String tempCol = null;
		Expression e1 = null;
		StringBuilder key = null;
		StringBuilder tCol = null;
		int index = 0;

		while (true) {

			if (this.tupleList == null || this.tupleList.isEmpty()) {
				tuple = this.optR.getTuple();
				if (tuple == null)
					return null;
				this.inputTuple = tuple;
			} else {
				tempTuple = this.getNextTuple(null);

				resultTuple = new Expression[tempTuple.length
						+ this.inputTuple.length];
				System.arraycopy(tempTuple, 0, resultTuple, 0, tempTuple.length);
				System.arraycopy(this.inputTuple, 0, resultTuple,
						tempTuple.length, this.inputTuple.length);
				return resultTuple;
			}

			if (tuple == null)
				return null;
			else {
				//colomnListItr = this.groupbyRightItmList.keySet().iterator();
				colomnListItr = this.rightItmList.iterator();
				key = null;
				while (colomnListItr.hasNext()) {
					col = (String) colomnListItr.next();
				
					if (this.tupleColumnMap.get(col) != null)
						col = tupleColumnMap.get(col);
					e1 = tuple[this.outputSchemaR.get(col)];
					
					if (key == null && e1 != null)
						key = new StringBuilder(e1.toString());
					else {
						if (e1 != null){
							key.append("|");
							key.append(e1.toString());
						}
					}
				}
				
				/* Merging tuple of left operator and right operator */
				if (this.leftTupleMap.get(key.toString()) != null) {
				
					if ((tempTuple = getNextTuple(key.toString())) != null) {

						resultTuple = new Expression[tempTuple.length
								+ this.inputTuple.length];
						System.arraycopy(tempTuple, 0, resultTuple, 0,
								tempTuple.length);
						System.arraycopy(this.inputTuple, 0, resultTuple,
								tempTuple.length, this.inputTuple.length);
					}
					if (resultTuple != null)
						break;
				}

			}
		}
		
		/*if(abhinit<=50 && state==5){
			
			Object[] obj = outputSchema.keySet().toArray();
			Integer val = 0;
			
		
			for(Expression key1 :resultTuple)
				System.out.print(key1+"|");
			
			System.out.println(" ");
			
			abhinit++;
		}*/
		
		return resultTuple;
	}

	@Override
	public void reset() {
		optR.reset();
	}

	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public HashMap<String, ArrayList<Expression[]>> groupByTuples(Operator opt,
			HashMap<String, String> groupbyItmList, HashMap<String, Integer> outputSchema) {
		Iterator colomnListItr = null;
		String col = null;
		ArrayList<Expression[]> listExpr = new ArrayList<Expression[]>();
		Expression e1 = null;
		Expression[] tuple = null;
		HashMap<String, ArrayList<Expression[]>> tupleMap = new HashMap<String, ArrayList<Expression[]>>();
		StringBuilder key = null;
		StringBuilder tCol = null;
		int count = 1;
		String tempCol = null;
		int index = 0;

		while ((tuple = opt.getTuple()) != null) {
			colomnListItr = groupbyItmList.keySet().iterator();
			key = null;
		
			while (colomnListItr.hasNext()) {
				col = (String) colomnListItr.next();

				/*if (this.hashTupleColumnMap == null)
					this.hashTupleColumnMap = this.getTupleMetaData(
							this.outputSchemaL, groupbyItmList);*/

				col = this.hashTupleColumnMap.get(col);
				index = outputSchema.get(col);
				e1 = tuple[index];

				if (key == null && e1 != null)
					key = new StringBuilder(e1.toString());
				else {
					if (e1 != null){
						key.append("|");
						key.append(e1.toString());
					}
				}
			}

			if ((listExpr = tupleMap.get(key.toString())) != null) {
				listExpr.add(tuple);
				tupleMap.put(key.toString(), listExpr);
			} else {
				listExpr = new ArrayList<Expression[]>();
				listExpr.add(tuple);
				/*if(state==4){
					System.out.println("Key:" + key);
				}*/
				tupleMap.put(key.toString(), listExpr);
			}
			count++;
		}

		return tupleMap;
	}

	public Expression[] getNextTuple(String key) {
		/*
		 * HashMap<String, Expression> tempTuple = null; HashMap<String,
		 * Expression> resultTuple = new HashMap<String, Expression>();
		 */
		Expression[] tempTuple = null;
		Expression[] resultTuple = null;

		if (this.tupleList == null
				|| (this.tupleList != null && this.tupleList.size() == 0)) {
			tupleList = new ArrayList<Expression[]>();
			if (this.leftTupleMap.get(key) != null)
				tupleList.addAll(this.leftTupleMap.get(key));
		}

		if (this.tupleList != null && !tupleList.isEmpty()) {
			tempTuple = tupleList.remove(0);
			resultTuple = tempTuple.clone();
		} else
			return null;

		return resultTuple;
	}

	public HashMap<String, String> getTupleMetaData(
			HashMap<String, Integer> schema,
			HashMap<String, String> groupbyItmList) {
		HashMap<String, String> groupbyColumnList = groupbyItmList;

		if (groupbyColumnList == null)
			return null;

		Iterator<String> itr = groupbyColumnList.keySet().iterator();
		String col = null;
		String tempCol = null;
		String key = null;
		StringBuilder tCol = null;
		int index = 0;
		HashMap<String, String> tupleColMap = new HashMap<String, String>();

		while (itr.hasNext()) {
			col = itr.next();

			tempCol = null;
			if (col.contains(".")) {
				tCol = new StringBuilder(col);
				index = col.indexOf(".");
				tempCol = tCol.substring(index + 1, col.length());
				tCol = null;
			}

			if (schema.get(col) != null)
				key = col;
			else if (schema.get(col.toLowerCase()) != null)
				key = col.toLowerCase();
			else if (schema.get(col.toUpperCase()) != null)
				key = col.toUpperCase();
			else if (schema.get(tempCol) != null)
				key = tempCol;
			else if (schema.get(tempCol.toLowerCase()) != null)
				key = tempCol.toLowerCase();
			else if (schema.get(tempCol.toUpperCase()) != null)
				key = tempCol.toUpperCase();
			else
				key = null;

			if (key != null)
				tupleColMap.put(col, key);
		}

		return tupleColMap;
	}
	
	ArrayList<String> sortSchema(HashMap<String,String> leftList,
			HashMap<String,String> rightList){
		StringBuilder colName = null;
		String tempL = null;
		String tempR= null;
		int index = 0;
		ArrayList<String> sortedList = new ArrayList<String>();
		
		for (String col : leftList.keySet()) {
			if(col.contains(".")) {
				colName = new StringBuilder(col);
				index = colName.indexOf(".");
				tempL = colName.substring(index+1, col.length());
			}
			else
				tempL = col;
			
			for(String key : rightList.keySet()){
				if(key.contains(".")){
					colName = new StringBuilder(key);
					index = colName.indexOf(".");
					tempR = colName.substring(index+1, key.length());
				}
				else
					tempR = key;
				
				if(tempR.equals(tempL))
					sortedList.add(key);
			}
		}
		
		return sortedList;
	}
}
