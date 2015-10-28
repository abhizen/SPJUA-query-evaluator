package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.buffalo.cse562.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class tuplegroupBy extends Eval {

    ArrayList<ColumnDefinition> inputSchema = null;
    ArrayList<ColumnDefinition> outputSchema = null;
    HashMap<String, ArrayList<HashMap<String, Expression>>> list = null;
    HashMap<String, Expression> tuple = null;
    ArrayList<HashMap<String, Expression>> tupleList = null;
    ArrayList<Object> expList = null;
    ArrayList<Object> groupList = null;
    String key = null;

    /*Function  aggFunc =null; 
     String aggCol =null;*/
    public tuplegroupBy(ArrayList<HashMap<String, Expression>> tupleList,
            ArrayList<Object> expList, ArrayList<Object> groupList,
            ArrayList<ColumnDefinition> inputSchema) {
        this.tupleList = tupleList;
        this.expList = expList;
        this.groupList = groupList;
        this.list = new HashMap<String, ArrayList<HashMap<String, Expression>>>();
        this.inputSchema = inputSchema;
        //System.out.println("Tuple Group by"+inputSchema);
    }

    public HashMap<String, ArrayList<HashMap<String, Expression>>>
            loadArrayList(ArrayList<HashMap<String, Expression>> tuplelist) {
        ArrayList<HashMap<String, Expression>> tempTupleList = tuplelist;
        Iterator<HashMap<String, Expression>> itr = tempTupleList.iterator();

        //Expression col = ((SelectExpressionItem)expList.get(0)).getExpression();
        Column col = (Column) this.groupList.get(0);
        ArrayList<HashMap<String, Expression>> listExpr = new ArrayList<HashMap<String, Expression>>();
        Expression e1 = null;

        while (itr.hasNext()) {
            tuple = itr.next();
            //Expression  e1 = (Expression)tuple.get(col);
            try {
				//System.out.println("TUPLEEE: " + tuple);				
                //System.out.println("COL: " + col);
                //System.out.println("Before calling eval");
                e1 = (Expression) this.eval(col);
                //System.out.println(col);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            //System.out.println("e1.tostring output" + e1.toString());

            if ((listExpr = list.get(e1.toString())) != null) {
                listExpr.add(tuple);
                //list.put(col, listExpr);
                list.put(e1.toString(), listExpr);
            } else {
                listExpr = new ArrayList<HashMap<String, Expression>>();
                listExpr.add(tuple);
                list.put(e1.toString(), listExpr);
            }
        }

        return list;
    }

    public HashMap<String, ArrayList<HashMap<String, Expression>>> multipleGrouping(HashMap<String, ArrayList<HashMap<String, Expression>>> tupleMap, String col) {

        String key = null;

        Map.Entry<String, ArrayList<HashMap<String, Expression>>> entry = null;
        ArrayList<HashMap<String, Expression>> tupleList = null;
        Iterator listItr = null;
        //HashMap<String, Expression> tuple =null;
        ArrayList<HashMap<String, Expression>> listExpr = null;

        HashMap<String, ArrayList<HashMap<String, Expression>>> tempTuple = tupleMap;
        Iterator<Entry<String, ArrayList<HashMap<String, Expression>>>> itr = tempTuple.entrySet().iterator();

        HashMap<String, ArrayList<HashMap<String, Expression>>> tempTupleMap
                = new HashMap<String, ArrayList<HashMap<String, Expression>>>();

        while (itr.hasNext()) {
            entry = (Entry<String, ArrayList<HashMap<String, Expression>>>) itr.next();
            //tupleList stores ArrayList
            tupleList = entry.getValue();

            //This iterator iterates  ArrayList 
            listItr = tupleList.iterator();

            while (listItr.hasNext()) {
                key = entry.getKey();
                tuple = (HashMap<String, Expression>) listItr.next();
				//System.out.println("Tuple :"+tuple);
                //key = key.concat(tuple.get(col).toString());
                Column ecol = new Column();
                ecol.setColumnName(col);

                try {
                    key = key.concat(this.eval(ecol).toString());
                    //System.out.println("key :"+key);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                if (tempTupleMap.get(key) != null) {
                    listExpr = tempTupleMap.get(key);
                    listExpr.add(tuple);
                    tempTupleMap.put(key, listExpr);
                } else {
                    listExpr = new ArrayList<HashMap<String, Expression>>();
                    listExpr.add(tuple);
                    tempTupleMap.put(key, listExpr);
                }

            }
        }
        return tempTupleMap;
    }

    public ArrayList<HashMap<String, Expression>> groupAgg(HashMap<String, ArrayList<HashMap<String, Expression>>> tupleMap) {
        //System.out.println("TupleMap :"+tupleMap);
        HashMap<String, ArrayList<HashMap<String, Expression>>> tempTuple = tupleMap;
        Iterator itr = tupleMap.entrySet().iterator();
        HashMap<String, ArrayList<HashMap<String, Expression>>> tempTupleMap
                = new HashMap<String, ArrayList<HashMap<String, Expression>>>();

        Map.Entry<String, ArrayList<HashMap<String, Expression>>> entry = null;
        ArrayList<HashMap<String, Expression>> tupleList = null;
        Iterator listItr = null;
        HashMap<String, Expression> tuple = null;
        ArrayList<HashMap<String, Expression>> outputtupleList = new ArrayList<HashMap<String, Expression>>();

        ArrayList<HashMap<String, Expression>> listExpr = null;

        while (itr.hasNext()) {
            entry = (Entry<String, ArrayList<HashMap<String, Expression>>>) itr.next();
            tupleList = entry.getValue();

            //if((this.aggFunc).getName().toString().equalsIgnoreCase("SUM")){
            SumGroupBy sumObj = new SumGroupBy(tupleList, this.expList, inputSchema);
            tuple = sumObj.getTuple();
            outputtupleList.add(tuple);
			//}

        }
        return outputtupleList;
    }

    @Override
    public LeafValue eval(Column c) throws SQLException {
        Table t = c.getTable();
        String col = c.getColumnName();
        //System.out.println("String col value in eval" + c.getColumnName());
        for (ColumnDefinition cd : this.inputSchema) {
            String schemaName = cd.getColumnName();
            //System.out.println(inputSchema);
            //System.out.println("String for column name in eval"+cd.getColumnName());

            //System.out.println(schemaName);
            if (schemaName.matches("(.*)" + col)) {
                //System.out.println("Match found");   
                col = cd.getColumnName();
                //System.out.println("Value of col after match found "+col);
            }
        }
        //System.out.println(col);
        //System.out.println(tuple);
        Expression val = this.tuple.get(col);
        //System.out.println("TUPLE"+tuple);
        //System.out.println(col); 
        //System.out.println("Value of val in eval"+val);

        return (LeafValue) val;

    }
}
