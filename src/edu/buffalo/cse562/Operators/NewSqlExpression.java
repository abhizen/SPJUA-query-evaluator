package edu.buffalo.cse562.Operators;

import java.util.ArrayList;
import java.util.HashMap;

import java.util.Iterator;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class NewSqlExpression {
	static HashMap<String, String> dataTypeMap = null;

	public static void loadSqlExpression(ArrayList<ColumnDefinition> schema) {
		if (dataTypeMap == null)
			dataTypeMap = new HashMap<String, String>();

		Iterator<ColumnDefinition> itr = schema.iterator();
		ColumnDefinition colDef = null;
		String colDataType = null;
		String dataType = null;

		while (itr.hasNext()) {
			colDef = itr.next();
			if (colDef != null)
				colDataType = colDef.getColDataType().getDataType();
			if (dataTypeMap.get(colDataType) == null) {

				if (colDataType.contains("(")) {
					int index = colDataType.indexOf("(");
					StringBuffer colType = new StringBuffer(colDataType);
					dataType = colType.substring(0, index).toUpperCase();
					colType = null;
					dataTypeMap.put(colDataType, dataType);
				} else {
					dataTypeMap.put(colDataType, colDataType.toUpperCase());
				}

			}
		}
	}

	public static Expression getExpression(int dataType, String expr) {
		LeafValue obj = null;
		String colDataType = null;
	
		/*INTEGER*/
		if (dataType == 1) {
			obj = new LongValue(expr);
			return (Expression) obj;
		}
		
		/*VARCHAR*/
		if (dataType==2) {
			StringBuilder strExpr = new StringBuilder(" ");
			strExpr.append(expr);
			strExpr.append(" ");

			obj = new StringValue(strExpr.toString());
			strExpr = null;

			return (Expression) obj;
		}

		/*CHAR*/
		if (dataType==3) {

			StringBuilder strExpr = new StringBuilder(" ");
			strExpr.append(expr);
			strExpr.append(" ");

			obj = new StringValue(strExpr.toString());
			strExpr = null;

			return (Expression) obj;
		}
		
		/*STRING*/
		if (dataType==4) {
			StringBuilder strExpr = new StringBuilder(" ");
			strExpr.append(expr);
			strExpr.append(" ");

			obj = new StringValue(strExpr.toString());
			strExpr = null;

			return (Expression) obj;
		}

		/*DECIMAL*/
		if (dataType==5) {
			obj = new DoubleValue(expr);
			return (Expression) obj;
		}
		
		/*DATE*/
		if (dataType==6) {
			StringBuilder strExpr = new StringBuilder(" ");
			strExpr.append(expr);
			strExpr.append(" ");

			obj = new DateValue(strExpr.toString());

			strExpr = null;

			return (Expression) obj;
		}

		return null;
	}
}
