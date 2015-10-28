package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.Eval;

public class LimitOperator extends Eval implements Operator {
	
	Operator opt = null;
	ArrayList<Object> condition = null;
	//ArrayList<ColumnDefinition> inputSchema = null;
	//ArrayList<ColumnDefinition> outputSchema = null;
	HashMap<String, Integer> inputSchema = null;
	HashMap<String, Integer> outputSchema = null;
	//HashMap<String,Expression> tuple = null;
	Expression[] tuple = null;
	//ArrayList<HashMap<String, Expression>> allTuples = null;
	ArrayList<Expression[]> allTuples = null;
	int limit;
	int tupleIndex;
	
	public LimitOperator(Operator op, ArrayList<Object> expr) {
		opt = op;
		condition =  expr;
		//tuple = new HashMap<String, Expression>();
		//allTuples = new ArrayList<HashMap<String, Expression>>();
		inputSchema = opt.getOutputSchema();
		outputSchema = inputSchema;
		tuple = new Expression[inputSchema.size()];
		allTuples = new ArrayList<Expression[]>();
		tupleIndex = 0;
		String list[] = null;
		
		String condStr = condition.toString();
		int lastIndex = condStr.lastIndexOf("]");
		//list = condStr.split(" ");
		StringTokenizer tmp = new StringTokenizer(condStr," ");
		String strLimit = null;
		
		while(tmp.hasMoreElements()){
			strLimit = (String) tmp.nextElement();
		}
		
		if(strLimit.contains("]")){
			lastIndex = strLimit.indexOf("]");
			strLimit = strLimit.substring(0, lastIndex);
		}
		
		limit = Integer.parseInt(strLimit);
		
		getAllTuples();
	}
	
	public void getAllTuples() {
		//HashMap<String, Expression> currTuple = new HashMap<String, Expression>();
		Expression[] currTuple;
		int count = 0;
		while (count < limit && ((currTuple = opt.getTuple()) != null)) {
			/*HashMap<String, Expression> thisTuple = new HashMap<String, Expression>();
			for (String s : currTuple.keySet()) {
				thisTuple.put(s, currTuple.get(s));
			}*/
			allTuples.add(currTuple);
			count++;
		}
		/*//System.out.println("----------- # allTuples = " + allTuples.size() + " -----------");
		for (int i = 0; i < allTuples.size(); i++) {
			//System.out.println("allTuples[" + i + "]: " + allTuples.get(i));
		}*/
	}
	
	//public ArrayList<ColumnDefinition> getOutputSchema(){
	public HashMap<String, Integer> getOutputSchema(){
		return outputSchema;
	}
	
	//public HashMap<String,Expression> getTuple() {
	public Expression[] getTuple() {
		//HashMap<String, Expression> returnTuple = null;
		Expression[] returnTuple = null;
		if (tupleIndex < allTuples.size()) {
			returnTuple = allTuples.get(tupleIndex);
			tupleIndex++;
		}
		return returnTuple;
	}

	@Override
	public void reset() {
		opt.reset();
	}

	@Override
	public LeafValue eval(Column c) throws SQLException {
		String col = c.getColumnName();
		int colIndex = this.inputSchema.get(col);
		Expression val = this.tuple[colIndex];
		return (LeafValue)val;
	}

}
