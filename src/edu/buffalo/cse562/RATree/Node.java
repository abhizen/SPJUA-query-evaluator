package edu.buffalo.cse562.RATree;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class Node{
    private ArrayList<Object> operation;
    private ArrayList<Object> secOperation;
    private String optName;
    private Node right;
    private Node left;
    private int priority = 0; 
    private ArrayList<ColumnDefinition> inputSchema = null;
    private ArrayList<ColumnDefinition> outputSchema = null;
    public int lock = 0;
    
    public Node(int priority){
        
        right=null;
        left = null; 
        this.priority = priority;
    }
    
    public void setRight(Node n){
        right = n;
    }
    
    public void setLeft(Node n){
        left = n;
    }
    
    public void setValue(ArrayList<Object>  operation,String optName){
        this.operation = operation; 
        this.optName = optName;
    }
    
    public void setPriority(int i){
         this.priority = i;
    }
    
    public void setsecOperation(ArrayList<Object>  operation){
    	this.secOperation = operation;
    }
    
    public void setSchema(ArrayList<ColumnDefinition> inputSchema){
    	this.inputSchema = inputSchema;
    }
   
    public void setLock(){
        this.lock = 1;
   }
    
    public void resetLock(){
        this.lock = 0;
   }
    
    public ArrayList<Object>  getsecOperation(){
    	return this.secOperation;
    }
   
    public String getOptName(){
        return this.optName;
    }
    
    public Object getValue(){
        return this.operation;
    }
    
    public int getPriority(){
        return this.priority;
    }
    
    public Node getRight(){
       return right;
    }
    
    public Node getLeft(){
       return left;
    }
    
    public ArrayList<ColumnDefinition> getSchema(){
    	this.outputSchema = this.inputSchema;
    	return this.outputSchema;
    }
    
    public boolean isSet(){
    	if(this.lock==1)
    		return true;
    	else
    		return false;
    }
}
