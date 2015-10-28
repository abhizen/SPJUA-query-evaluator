package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.Eval;
import java.util.Map;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class MinMax extends Eval {

    HashMap<String, Integer> inputSchema = null;
    HashMap<String, Integer> outputSchema = null;
    Expression[] tuple = null;
    ArrayList<Expression[]> tupleList = null;
    Expression exp = null;
    Operator opt = null;
    int index = 0;
    char flag = 0;

    public MinMax(ArrayList<Expression[]> tupleList, Expression exp, char flag, HashMap<String, Integer> inputSchema) {
        this.exp = exp;
        this.tupleList = tupleList;
        this.flag = flag;
        this.inputSchema = inputSchema;
        //System.out.println("Sum" + inputSchema);
        //System.out.println("Sum" + tupleList);
    }

    public Expression getTuple() {
        ExpressionList lst = ((Function) exp).getParameters();
        List<Expression> expList = (List<Expression>) lst.getExpressions();
        LeafValue lf = null;
        double val = 0;
        double min = 0;
        double max = 0;
        int index = 0;
        DoubleValue dValue = null;

        Iterator<Expression[]> itr = tupleList.iterator();

        while (itr.hasNext()) {
            tuple = itr.next();
            for (Expression e : expList) {

                String match = e.toString();
                if (match.contains("+") || match.contains("-") || match.contains("*") || match.contains("/")) {
                    try {
                        lf = this.eval(e);
                        val = lf.toDouble();
                        if (index == 0) {
                            min = val;
                            max = val;
                        } else {
                            if (min > val) {
                                min = val;
                            }
                            if (max < val) {
                                max = val;
                            }
                        }

                        //System.out.println(lf);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }

                } else {
                    for (Map.Entry entry : inputSchema.entrySet()) {
                        String schemaName = entry.getKey().toString();

                        //System.out.println(schemaName);
                        if (schemaName.matches("(.*)" + match)) {

                            match = entry.getKey().toString();
                        }
                    }

                    //System.out.println(match);
                    lf = (LeafValue) tuple[inputSchema.get(match)];
                    if (lf != null) {
                        try {
                            val = lf.toDouble();
                            if (index == 0) {
                                min = val;
                                max = val;
                            } else {
                                if (min > val) {
                                    min = val;
                                }
                                if (max < val) {
                                    max = val;
                                }
                            }
                        } catch (InvalidLeaf e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                }
            }

            index++;
        }

        if (flag == 'Y') {
            dValue = new DoubleValue(min);
            //tuple = new HashMap<String, Expression>();
            //tuple.put(this.exp.toString(), dValue);
        } else {
            dValue = new DoubleValue(max);
            //tuple = new HashMap<String, Expression>();
            //tuple.put(this.exp.toString(), dValue);
        }

        return dValue;
    }

    @Override
    public LeafValue eval(Column c) throws SQLException {

        String col = c.getColumnName();
        for (Map.Entry entry : inputSchema.entrySet()) {
            String schemaName = entry.getKey().toString();

            //System.out.println(schemaName);
            if (schemaName.matches("(.*)" + col)) {

                col = entry.getKey().toString();
            }
        }

        Expression val = this.tuple[inputSchema.get(col)];
        //System.out.println(col); 
        //System.out.println(val);

        return (LeafValue) val;
    }

}
