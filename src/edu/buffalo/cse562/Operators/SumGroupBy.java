package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class SumGroupBy extends Eval {

    ArrayList<ColumnDefinition> inputSchema = null;
    ArrayList<ColumnDefinition> outputSchema = null;
    HashMap<String, Expression> tuple = null;
    ArrayList<HashMap<String, Expression>> tupleList = null;
    ArrayList<Object> expList = null;
    Expression exp = null;
    Operator opt = null;
    int index = 0;
    HashMap<String, Expression> resultTuple = null;
    HashMap<String,ColumnDefinition> inputschemaMap = new HashMap<String,ColumnDefinition>();
//HashMap<String, Expression> sum=null;

    public SumGroupBy(ArrayList<HashMap<String, Expression>> tupleList, ArrayList<Object> expList,
            ArrayList<ColumnDefinition> inputSchema) {
        this.expList = expList;
        this.tupleList = tupleList;
        this.inputSchema = inputSchema;
        //System.out.println(inputSchema);
        this.resultTuple = new HashMap<String, Expression>();
        this.loadSchemaMap();
    }

    public HashMap<String, Expression> getTuple() {
        LeafValue lf = null;
        int index = 0;
        double val = 0;
        double sum = 0;
        double avg = 0;
        double min = 0;
        double max = 0;
        String funcName = null;
        StringBuilder tempfuncName = null;
        int dotIndex = 0;

        DoubleValue dValue = null;
        HashMap<String, Expression> tempTuple = new HashMap<String, Expression>();
        HashMap<String, Expression> avgMap = null;

        
        for(Object e :expList){
                String alias = null;
                alias = ((SelectExpressionItem) e).getAlias();
                Expression ep = ((SelectExpressionItem) e).getExpression();
                if (alias == null) {
                    resultTuple.put(ep.toString(), new DoubleValue(sum));
                    
                }
                else{
                    resultTuple.put(alias, new DoubleValue(sum));
                }

        }
        Iterator<HashMap<String, Expression>> itr = tupleList.iterator();
        while (itr.hasNext()) {
            tuple = itr.next();
            index++;
            
            for (Object e : expList) {
                //System.out.println(resultTuple);
                String alias = null;
                alias = ((SelectExpressionItem) e).getAlias();
                Expression ep = ((SelectExpressionItem) e).getExpression();
                //System.out.println("Expression in sum group by"+ep);
                if (ep instanceof Function) {
                	funcName= ((Function) ep).getName().toUpperCase();
                    if(funcName.contains("(")){
            			dotIndex = funcName.indexOf("(");
            			tempfuncName = new StringBuilder(funcName);
            			funcName = tempfuncName.substring(0, dotIndex);
            			tempfuncName = null;
            		}
                    ExpressionList lst = ((Function) ep).getParameters();

                    if (!((Function) ep).isAllColumns()) {

                        List<Expression> expList = (List<Expression>) lst.getExpressions();
                        
                        for (Expression l : expList) {
                            val = 0;

                            //lf = (LeafValue) tuple.get(l.toString());
                            try {
                                lf = this.eval(l);
                                if (lf != null && !(lf instanceof StringValue)) {
                                    val = lf.toDouble();
                                }
                            } catch (InvalidLeaf e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                            } catch (SQLException e2) {
                                e2.printStackTrace();
                            }
                            //System.out.println("Value of l:::::::"+l);
						/*index++;*/
                            
                            
                            //if (((Function) ep).getName().matches("SUM(.*)") || ((Function) ep).getName().matches("sum(.*)")) {
                            if (funcName.equals("SUM")){
                                //System.out.println("Inside sum");

                                if (alias == null) {
                                    sum=0;
                                    sum = Double.parseDouble(resultTuple.get(ep.toString()).toString());
                                    

                                    sum = sum + val;

                                    resultTuple.put(ep.toString(), new DoubleValue(sum));
                                    tempTuple.put(ep.toString(), new DoubleValue(sum));
                                    sum=0;
                                } else {
                                    sum=0;
                                    //System.out.println(resultTuple);
                                    sum = Double.parseDouble(resultTuple.get(alias).toString());
                                    
                                    //System.out.println("Sum and Val : "+sum+" "+val);
                                    sum = sum + val;
                                    //System.out.println(alias);
                                    
                                    //System.out.println(sum);
                                    
                                    resultTuple.put(alias, new DoubleValue(sum));
                                    tempTuple.put(alias, new DoubleValue(sum));
                                    //System.out.println("After operation"+resultTuple);
                                    sum=0;
                                }

                            }
                            //if (((Function) ep).getName().matches("AVG(.*)") || ((Function) ep).getName().matches("avg(.*)")) {
                            	if(funcName.equals("AVG")){
                            		if (index == 1) {
                            			avgMap = new HashMap<String, Expression>();
                            		}

                                
                                //System.out.println(val);
                                if (alias == null) {
                                    avg=0;
                                    avg = Double.parseDouble(resultTuple.get(ep.toString()).toString());
                                    avg = avg + val;
                                    
                                    tempTuple.put(ep.toString(), new DoubleValue(avg));
                                    avgMap.put(ep.toString(), new DoubleValue(index));
                                    resultTuple.put(alias, new DoubleValue(avg));
                                } else {
                                    avg=0;
                                    avg = Double.parseDouble(resultTuple.get(alias).toString());
                                    avg = avg + val;
                                    
                                    tempTuple.put(alias, new DoubleValue(avg));
                                    avgMap.put(alias, new DoubleValue(index));
                                    resultTuple.put(alias, new DoubleValue(avg));
                                }
                            }
                            //if (((Function) ep).getName().matches("MIN(.*)") || ((Function) ep).getName().matches("min(.*)")) {
                            	if(funcName.equals("MIN")){
                                if (index == 1) {
                                    min = val;
                                } else if (min > val) {
                                    min = val;
                                }
                                if (alias == null) {
                                    tempTuple.put(ep.toString(), new DoubleValue(min));
                                } else {
                                    tempTuple.put(alias, new DoubleValue(min));
                                }

                            }
                            //if (((Function) ep).getName().matches("MAX(.*)") || ((Function) ep).getName().matches("max(.*)")) {
                            	if(funcName.equals("MAX")){
                                if (index == 1) {
                                    max = val;
                                } else if (max < val) {
                                    max = val;
                                }
                                if (alias == null) {
                                    tempTuple.put(ep.toString(), new DoubleValue(max));
                                } else {
                                    tempTuple.put(alias, new DoubleValue(max));
                                }
                            }
                            //if (((Function) ep).getName().matches("COUNT(.*)") || ((Function) ep).getName().matches("count(.*)")) {
                            	if(funcName.equals("COUNT")){
                                if (alias == null) {
                                    tempTuple.put(ep.toString(), new DoubleValue(index));
                                } else {
                                    tempTuple.put(alias, new DoubleValue(index));
                                }
                            }

                        }
                    } else {
                        //if (((Function) ep).getName().matches("COUNT(.*)") || ((Function) ep).getName().matches("count(.*)")) {
                    	if(funcName.equals("COUNT")){
                            //index++;

                            if (alias == null) {
                                tempTuple.put(ep.toString(), new DoubleValue(index));
                            } else {
                                tempTuple.put(alias, new DoubleValue(index));
                            }
                        }

                    }
                } else {
                    //Expression e2 = tuple.get(ep.toString());
                    try {
                        LeafValue e2 = this.eval(ep);
                        tempTuple.put(ep.toString(), (Expression) e2);
                    } catch (SQLException e3) {
                        e3.printStackTrace();
                    }

                }
            }
        }

        if (avgMap != null) {
            DoubleValue count = null;
            double sumVal = 0;
            Iterator avgMapItr = avgMap.keySet().iterator();
            while (avgMapItr.hasNext()) {
                String key = avgMapItr.next().toString();

                count = (DoubleValue) avgMap.get(key);

                //System.out.println(key);
                try {
                    sumVal = ((LeafValue) tempTuple.get(key)).toDouble();
                } catch (InvalidLeaf e1) {
                    e1.printStackTrace();
                }
                //sumVal = ((DoubleValue)this.eval(new StringValue(key))).getValue();
                sumVal = sumVal / (count.getValue());

                //sumVal = ((DoubleValue)tempTuple.get(key)).getValue()/(count.getValue());
                tempTuple.put(key, new DoubleValue(sumVal));
            }
        }
        //System.out.println("Temp Tuple:"+tempTuple);
        return tempTuple;
    }

    public LeafValue eval(Column c) throws SQLException {
        String col = c.getColumnName().toUpperCase();
        
        col = inputschemaMap.get(col).getColumnName();
        Expression val = this.tuple.get(col);

        return (LeafValue) val;
    }
    public void loadSchemaMap(){
    	StringBuilder colName = null; 
    	String col = null;
    	int index = 0;
    	
    	for (ColumnDefinition cd : this.inputSchema) {
    		col = cd.getColumnName().toUpperCase();
    		if(col.contains(".")){
    			index = col.indexOf(".");
    			colName = new StringBuilder(col);
    			col = colName.substring(index+1, col.length());
    			colName = null;
    		}
    		
    		if(this.inputschemaMap.get(col)==null)
    			this.inputschemaMap.put(col, cd);
    	}
    }
}
