package edu.buffalo.cse562.Operators;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;

//public class OrderByComparator implements Comparator<HashMap<String, Expression>> {
public class OrderByComparator implements Comparator<Expression[]> {
	
	ArrayList<String[]> sortConditions = new ArrayList<>();
	HashMap<String, Integer> schema = new HashMap<String, Integer>();
	
	public OrderByComparator(ArrayList<String[]> orderByCols, HashMap<String, Integer> orderSchema) {
		sortConditions = orderByCols;
		schema = orderSchema;
	}

	@Override
	//public int compare(HashMap<String, Expression> tuple1, HashMap<String, Expression> tuple2) {
	public int compare(Expression[] tuple1, Expression[] tuple2) {
		int value = 1;
		int index = 0;
		if (sortConditions.size() > 0) {
			value = compareTuples(tuple1, tuple2, index);
			//System.out.println("----| col: " + sortConditions.get(index)[0] + ", value: " + value + " |----");
			index++;
			if (value == 0 && sortConditions.size() > 1) {
				while (value == 0 && index < sortConditions.size()) {
					value = compareTuples(tuple1, tuple2, index);
					index++;
				}
			}
			if (value == 0)
				value++;
		}
		//System.out.println("----| value: " + value + " |----\n");
		
		return value;
	}
	
	//private int compareTuples(HashMap<String, Expression> tuple1, HashMap<String, Expression> tuple2, int sortColIndex) {
	private int compareTuples(Expression[] tuple1, Expression[] tuple2, int sortColIndex) {
		int val = 0;
		String col = sortConditions.get(sortColIndex)[0];
		Boolean isAsc = Boolean.parseBoolean(sortConditions.get(sortColIndex)[1]);
		String dataType = sortConditions.get(sortColIndex)[2];
		int colIndex = schema.get(col);
		/*String value1 = tuple1.get(col).toString();
		String value2 = tuple2.get(col).toString();*/
		String value1 = tuple1[colIndex].toString();
		String value2 = tuple2[colIndex].toString();
		if (dataType == null) {
			if (tuple1[colIndex] instanceof DoubleValue) {
				dataType = "double";
			} else if (tuple1[colIndex] instanceof StringValue) {
				dataType = "string";
			} else if (tuple1[colIndex] instanceof DateValue) {
				dataType = "date";
			}
			String[] str = new String[3];
			str[0] = col;
			str[1] = isAsc.toString();
			str[2] = dataType;
			sortConditions.add(sortColIndex, str);
			sortConditions.remove(sortColIndex+1);
		}
		
		switch (dataType) {
			case "int":
				val = Integer.compare(Integer.parseInt(value1), Integer.parseInt(value2));
				break;
			case "double":
				val = Double.compare(Double.parseDouble(value1), Double.parseDouble(value2));
				break;
			case "decimal":
				val = Double.compare(Double.parseDouble(value1), Double.parseDouble(value2));
				break;
			case "string":
				val = value1.compareToIgnoreCase(value2);
				break;
			case "char":
				val = value1.compareToIgnoreCase(value2);
				break;
			case "varchar":
				val = value1.compareToIgnoreCase(value2);
				break;
			case "date":
				val = Date.valueOf(value1).compareTo(Date.valueOf(value2));
				break;
			case "INT":
				val = Integer.compare(Integer.parseInt(value1), Integer.parseInt(value2));
				break;
			case "DOUBLE":
				val = Double.compare(Double.parseDouble(value1), Double.parseDouble(value2));
				break;
			case "DECIMAL":
				val = Double.compare(Double.parseDouble(value1), Double.parseDouble(value2));
				break;
			case "STRING":
				val = value1.compareToIgnoreCase(value2);
				break;
			case "CHAR":
				val = value1.compareToIgnoreCase(value2);
				break;
			case "VARCHAR":
				val = value1.compareToIgnoreCase(value2);
				break;
			case "DATE":
				val = Date.valueOf(value1).compareTo(Date.valueOf(value2));
				break;
			default:
				break;
		}
		
		if (!isAsc)
			val *= -1;
		
		return val;
	}
	
}