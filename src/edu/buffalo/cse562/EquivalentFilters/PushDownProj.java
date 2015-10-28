package edu.buffalo.cse562.EquivalentFilters;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.RATree.Node;

public class PushDownProj implements Visitor{
	public void visit(Node parent, Node child, String link){
		Node nParent = parent;
		Node nChild = child;
		Node newParent = null;
		Expression condition = null;
		ArrayList<Object> operation =null;
		if(nChild==null)
			return;
		
		
		if(nChild.getOptName().equals("SELECT")){
			operation = (ArrayList<Object>) nChild.getValue();
			condition = (Expression) operation.get(0);
			newParent = nChild.getRight();
			
			
				if(newParent.getLeft()!=null && checkSchema(nChild.getSchema(),
						newParent.getSchema())){
					pushDown(parent,child,child.getRight(),link,"LEFT");
				}
				if(newParent.getRight()!=null && checkSchema(nChild.getSchema(),
						newParent.getSchema())){
					pushDown(parent,child,child.getRight(),link,"RIGHT");
				}
			
				
		}
	}
	
	public boolean checkSchema(ArrayList<ColumnDefinition> parentSchema, 
			ArrayList<ColumnDefinition> childSchema){
		int flag = 0;
		
		/*if(parentSchema==null)
			return false;
		
		if(childSchema==null)
			return false;*/
		
		for(ColumnDefinition parentColDef : parentSchema){
			flag = 0;
			for(ColumnDefinition childColDef : childSchema){
				if(parentColDef.getColumnName().toUpperCase()
						.equals(childColDef.getColumnName().toUpperCase()))
					flag =1;
			}
			
			if(flag==0)
				return false;
		}
		
		return true;
	}
	
	public void pushDown(Node parent,Node child,Node newParent, String link,String newLink){
		Node temp = child;
		if(link.toUpperCase().equals("LEFT")){
			parent.setLeft(child.getRight());
		}
		else
			parent.setRight(child.getRight());
			
		if(newLink.toUpperCase().equals("LEFT")){
			child.setRight(newParent.getLeft());
			newParent.setLeft(child);
		}
		else{
			child.setRight(newParent.getRight());
			newParent.setRight(child);
		}
			
	}
	
	

}
