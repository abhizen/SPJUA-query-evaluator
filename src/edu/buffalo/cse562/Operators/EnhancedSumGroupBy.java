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

public class EnhancedSumGroupBy extends Eval {

	HashMap<String, Integer> inputSchema = null;
	HashMap<String, Integer> outputSchema = null;
	Expression[] tuple = null;
	ArrayList<Expression[]> tupleList = null;
	ArrayList<Object> expList = null;
	Expression exp = null;
	Operator opt = null;
	int index = 0;
	Expression[] resultTuple = null;
	HashMap<String, String> inputschemaMap = new HashMap<String, String>();
	HashMap<String, Expression> avgMap = null;

	public EnhancedSumGroupBy(ArrayList<Expression[]> tupleList,
			ArrayList<Object> expList, HashMap<String, Integer> inputSchema,
			HashMap<String, Integer> outputSchema) {
		this.expList = expList;
		this.tupleList = tupleList;
		this.inputSchema = inputSchema;
		this.resultTuple = new Expression[expList.size()];
		this.outputSchema = outputSchema;
		this.loadSchemaMap();
		// System.out.println("schema map"+inputschemaMap);
	}

	public void setTupleList(ArrayList<Expression[]> tupleList) {
		this.tupleList = tupleList;
	}

	public Expression[] getTuple() {
		return this.resultTuple;
	}

	public void calcTuple() {
		LeafValue lf = null;
		// int index = 0;
		double val = 0;
		double sum = 0;
		double avg = 0;
		double min = 0;
		double max = 0;
		String funcName = null;
		StringBuilder tempfuncName = null;
		int dotIndex = 0;
		int schemaIndex = 0;

		DoubleValue dValue = null;
		Iterator<Expression[]> itr = tupleList.iterator();
		while (itr.hasNext()) {
			tuple = itr.next();
			index++;

			for (Object e : expList) {

				String alias = null;
				alias = ((SelectExpressionItem) e).getAlias();
				Expression ep = ((SelectExpressionItem) e).getExpression();

				// Extract function name
				if (ep instanceof Function) {
					funcName = ((Function) ep).getName().toUpperCase();
					if (funcName.contains("(")) {
						dotIndex = funcName.indexOf("(");
						tempfuncName = new StringBuilder(funcName);
						funcName = tempfuncName.substring(0, dotIndex);
						tempfuncName = null;
					}
					ExpressionList lst = ((Function) ep).getParameters();

					if (!((Function) ep).isAllColumns()) {

						List<Expression> expList = (List<Expression>) lst
								.getExpressions();

						for (Expression l : expList) {
							val = 0;

							// lf = (LeafValue) tuple.get(l.toString());
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

							/* index++; */

							// if (((Function) ep).getName().matches("SUM(.*)")
							// || ((Function) ep).getName().matches("sum(.*)"))
							// {
							if (funcName.equals("SUM")) {

								if (alias == null
										&& this.outputSchema.get(ep.toString()) != null) {
									sum = 0;
									// sum =
									// Double.parseDouble(resultTuple.get(ep.toString()).toString());
									schemaIndex = this.outputSchema.get(ep
											.toString());
									// sum =
									// ((DoubleValue)resultTuple.get(ep.toString())).getValue();
									if (resultTuple[schemaIndex] == null)
										sum = 0;
									else
										sum = ((DoubleValue) resultTuple[schemaIndex])
												.getValue();
									sum = sum + val;
									// resultTuple.put(ep.toString(), new
									// DoubleValue(sum));
									resultTuple[schemaIndex] = new DoubleValue(
											sum);
									// tempTuple.put(ep.toString(), new
									// DoubleValue(sum));
									sum = 0;
								} else if (this.outputSchema.get(alias) != null) {
									sum = 0;
									// sum =
									// Double.parseDouble(resultTuple.get(alias).toString());
									schemaIndex = this.outputSchema.get(alias);
									if (resultTuple[schemaIndex] == null)
										sum = 0;
									else
										sum = ((DoubleValue) resultTuple[schemaIndex])
												.getValue();

									sum = sum + val;
									resultTuple[schemaIndex] = new DoubleValue(
											sum);
									// tempTuple.put(alias, new
									// DoubleValue(sum));
									sum = 0;
								}
								continue;
							}

							if (funcName.equals("AVG")) {
								if (index == 1) {
									avgMap = new HashMap<String, Expression>();
								}

								if (alias == null
										&& this.outputSchema.get(ep.toString()) != null) {
									avg = 0;
									schemaIndex = this.outputSchema.get(ep
											.toString());
									// avg =
									// Double.parseDouble(resultTuple.get(ep.toString()).toString());
									if (resultTuple[schemaIndex] == null)
										avg = 0;
									else
										avg = ((DoubleValue) resultTuple[schemaIndex])
												.getValue();

									if (avgMap.get(ep.toString()) != null) {
										avg = avg
												* ((DoubleValue) avgMap.get(ep
														.toString()))
														.getValue();
									}

									avg = avg + val;

									if (avgMap.get(ep.toString()) != null) {
										int tempCount = (int) ((DoubleValue) avgMap
												.get(ep.toString())).getValue();
										tempCount = tempCount + 1;
										avg = (double) avg / tempCount;
										avgMap.put(ep.toString(),
												new DoubleValue(tempCount));
									} else {
										avgMap.put(ep.toString(),
												new DoubleValue(1));
									}
									// tempTuple.put(ep.toString(), new
									// DoubleValue(avg));
									// resultTuple.put(ep.toString(), new
									// DoubleValue(avg));
									resultTuple[schemaIndex] = new DoubleValue(
											avg);

									continue;
								} else if (this.outputSchema.get(alias) != null) {
									avg = 0;
									schemaIndex = this.outputSchema.get(alias);

									// avg =
									// Double.parseDouble(resultTuple.get(alias).toString());
									if (resultTuple[schemaIndex] == null)
										avg = 0;
									else
										avg = ((DoubleValue) resultTuple[schemaIndex])
												.getValue();

									if (avgMap.get(alias) != null) {
										avg = avg
												* ((DoubleValue) avgMap
														.get(alias)).getValue();
									}

									avg = avg + val;

									if (avgMap.get(alias) != null) {
										int tempCount = (int) ((DoubleValue) avgMap
												.get(alias)).getValue();
										tempCount = tempCount + 1;
										avg = (double) avg / tempCount;
										avgMap.put(alias, new DoubleValue(
												tempCount));
									} else {
										avgMap.put(alias, new DoubleValue(1));
									}
									// tempTuple.put(alias, new
									// DoubleValue(avg));
									// resultTuple.put(alias, new
									// DoubleValue(avg));
									resultTuple[schemaIndex] = new DoubleValue(
											avg);
								}

								continue;
							}

							if (funcName.equals("MIN")) {
								/* if (index == 1) { */
								if (this.outputSchema.get(ep.toString()) != null) {
									schemaIndex = this.outputSchema.get(ep
											.toString());

									if (resultTuple[schemaIndex] == null)
										min = val;
									else
										min = ((DoubleValue) resultTuple[schemaIndex])
												.getValue();

									if (min > val)
										min = val;
								} else if (alias != null
										&& this.outputSchema.get(alias) != null) {
									schemaIndex = this.outputSchema.get(alias);

									if (resultTuple[schemaIndex] == null)
										min = val;
									else
										min = ((DoubleValue) resultTuple[schemaIndex])
												.getValue();

									if (min > val)
										min = val;
								} else
									min = val;

								if (alias == null) {

									schemaIndex = this.outputSchema.get(ep
											.toString());
									resultTuple[schemaIndex] = new DoubleValue(
											min);
								} else {
									schemaIndex = this.outputSchema.get(alias);
									resultTuple[schemaIndex] = new DoubleValue(
											min);
								}
								continue;
							}

							if (funcName.equals("MAX")) {
								
									if (this.outputSchema.get(ep.toString()) != null) {
										schemaIndex = this.outputSchema.get(ep
												.toString());

										if (resultTuple[schemaIndex] == null)
											max = val;
										else
											max = ((DoubleValue) resultTuple[schemaIndex])
													.getValue();

										if (max < val)
											max = val;
									} else if (alias != null
											&& this.outputSchema.get(alias) != null) {
										schemaIndex = this.outputSchema
												.get(alias);

										if (resultTuple[schemaIndex] == null)
											max = val;
										else
											max = ((DoubleValue) resultTuple[schemaIndex])
													.getValue();

										if (max < val)
											max = val;
									} else
										max = val;
								 
							
								if (alias == null) {
									
									schemaIndex = this.outputSchema.get(ep
											.toString());
									resultTuple[schemaIndex] = new DoubleValue(
											max);

								} else {
									schemaIndex = this.outputSchema.get(alias);
									resultTuple[schemaIndex] = new DoubleValue(
											max);
								}
								continue;
							}

							if (funcName.equals("COUNT")) {

								if (alias == null) {
									int tempCount = 1;

									if (this.outputSchema.get(ep.toString()) != null) {
										schemaIndex = this.outputSchema.get(ep
												.toString());
										if (resultTuple[schemaIndex] != null)
											tempCount = 1 + (int) ((DoubleValue) resultTuple[schemaIndex])
													.getValue();
									}

									resultTuple[schemaIndex] = new DoubleValue(
											tempCount);
								} else {
									int tempCount = 1;

									if (this.outputSchema.get(alias) != null) {
										schemaIndex = this.outputSchema.get(ep
												.toString());
										if (resultTuple[schemaIndex] != null)
											tempCount = 1 + (int) ((DoubleValue) resultTuple[schemaIndex])
													.getValue();
									}

									resultTuple[schemaIndex] = new DoubleValue(
											tempCount);
								}
								continue;
							}

						}
					} else {

						if (funcName.equals("COUNT")) {
							// index++;

							if (alias == null) {
								int tempCount = 1;

								if (this.outputSchema.get(ep.toString()) != null) {
									schemaIndex = this.outputSchema.get(ep
											.toString());
									if (resultTuple[schemaIndex] != null) {
										tempCount = (int) ((DoubleValue) resultTuple[schemaIndex])
												.getValue();
										tempCount = tempCount + 1;
									}
								}
								// tempTuple.put(ep.toString(), new
								// DoubleValue(tempCount));
								resultTuple[schemaIndex] = new DoubleValue(
										tempCount);
								continue;

							} else {
								int tempCount = 1;

								if (this.outputSchema.get(alias) != null) {
									schemaIndex = this.outputSchema.get(alias);
									if (resultTuple[schemaIndex] != null) {
										tempCount = (int) ((DoubleValue) resultTuple[schemaIndex])
												.getValue();
										tempCount = tempCount + 1;
									}
								}
								// tempTuple.put(alias, new
								// DoubleValue(tempCount));
								resultTuple[schemaIndex] = new DoubleValue(
										tempCount);
								continue;
							}
						}

					}
				} else {
					// Expression e2 = tuple.get(ep.toString());
					try {
						LeafValue e2 = this.eval(ep);
						// tempTuple.put(ep.toString(), (Expression) e2);
						// resultTuple.put(ep.toString(), (Expression) e2);
						schemaIndex = this.outputSchema.get(ep.toString());
						resultTuple[schemaIndex] = (Expression) e2;
					} catch (SQLException e3) {
						e3.printStackTrace();
					}

				}
			}
		}

	}

	public LeafValue eval(Column c) throws SQLException {
		String col = c.getColumnName().toUpperCase();
		Expression val = null;
		int schemaIndex = 0;

		if (this.inputSchema.get(col) != null) {
			schemaIndex = this.inputSchema.get(col);
			val = tuple[schemaIndex];
		} else {
			col = inputschemaMap.get(col);
			schemaIndex = this.inputSchema.get(col);
			val = tuple[schemaIndex];
		}

		return (LeafValue) val;
	}

	public void loadSchemaMap() {
		StringBuilder colName = null;
		String col = null;
		int index = 0;

		for (String cd : this.inputSchema.keySet()) {
			col = cd.toUpperCase();
			if (col.contains(".")) {
				index = col.indexOf(".");
				colName = new StringBuilder(col);
				col = colName.substring(index + 1, col.length());
				colName = null;
			}

			if (this.inputschemaMap.get(col) == null)
				this.inputschemaMap.put(col, cd);
		}
	}
}
