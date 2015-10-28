package edu.buffalo.cse562.SchemaFilters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;
import edu.buffalo.cse562.RATree.Node;
import edu.buffalo.cse562.SqlParser.TablesO;

public class JoinFilter extends Filter {

	TablesO table = null;
	private ArrayList<ColumnDefinition> rSchema = null;
	private ArrayList<ColumnDefinition> lSchema = null;
	ArrayList<ColumnDefinition> resultSchema = null;
	Node right = null; 
	Node left = null;
	
	static
	{
		FilterFactorySingleton.getFactoryInstance()
			.getFactory().registerFilter("Join",new JoinFilter());
	}

	public Filter createFilter(){
		return new JoinFilter();
	}
	
	public void setChild(Node left,Node right){
		this.right = right;
		this.left = left;
	}
	
	public void loadSchema(ArrayList<ColumnDefinition> rSchema, 
			ArrayList<ColumnDefinition> lSchema){

		this.rSchema = rSchema;
		this.lSchema = lSchema;
		this.resultSchema = new ArrayList<ColumnDefinition>();
		StringBuilder col = null;
		ColumnDefinition colDef = null;
		int index = 0;
		Iterator itr = null;
		
		if(right!=null && right.getOptName().equals("FROM") 
				&& !this.rSchema.get(0).getColumnName().contains(".")){
			Object rObj = ((ArrayList)right.getValue()).get(0);
			
			itr = this.rSchema.iterator();
			
			while(itr.hasNext()){
				colDef = (ColumnDefinition) itr.next();
				
				if(((Table)rObj).getAlias()!=null)
					col = new StringBuilder(((Table)rObj).getAlias());
				else
					col = new StringBuilder(((Table)rObj).getName());
				
				col.append(".");
				col.append(colDef.getColumnName());
				colDef.setColumnName(col.toString().toUpperCase());
				this.resultSchema.add(colDef);
				index++;
			}
		}
		else{
			this.resultSchema.addAll(this.lSchema);
			index = this.resultSchema.size();
		}
		
		if( left!=null && left.getOptName().equals("FROM")
				&& !this.lSchema.get(0).getColumnName().contains(".")){
			//if(this.lSchema.get(0).getColumnName().contains("."))
				
			Object lObj = ((ArrayList)left.getValue()).get(0);
			
			itr = this.lSchema.iterator();
			
			while(itr.hasNext()){
				colDef = (ColumnDefinition) itr.next();
				
				if(((Table)lObj).getAlias()!=null)
					col = new StringBuilder(((Table)lObj).getAlias());
				else
					col = new StringBuilder(((Table)lObj).getName());
				
				col.append(".");
				col.append(colDef.getColumnName());
				colDef.setColumnName(col.toString().toUpperCase());
				this.resultSchema.add(colDef);
				index++;
			}
		}
		else{
			 this.resultSchema.addAll(this.rSchema);
		}
      
		
	}
	

	public ArrayList<ColumnDefinition> getOutputSchema(){
		return resultSchema;
	}
}
