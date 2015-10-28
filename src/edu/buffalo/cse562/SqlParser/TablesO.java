package edu.buffalo.cse562.SqlParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;


public class TablesO{

    //Stores table name
    private String tableName;
    //stores number of columns
    private int numCol;
    //Save the data file path

    private File filepath;
    private FileReader fr;
    private BufferedReader bufr;

    
    //Stores column definitions
    private ArrayList<ColumnDefinition> colDef;
    
    //Stores Schema
    //HashMap<String, ColumnDefinition> schema =new HashMap<String, ColumnDefinition>();
    private ArrayList<ColumnDefinition> schema =new ArrayList<ColumnDefinition>();
    
    //Returns to String of tables schema
    String tableToString;
    public TablesO() {
        
    }
    //Constructor for tables
    public TablesO(String tableName, int numCol){
        this.tableName=tableName;
        this.numCol=numCol;
        this.bufr=null;
        this.fr=null;
         
    }
    
    public String getTableName(){
        return tableName;
    }
    
    public File getFilePath(){
        return filepath;
    }
    
    public void setFilePath(File filePath){
        this.filepath = filePath;
    }
    
    public void setColumnDef(ArrayList<ColumnDefinition> colDef){
    	this.colDef = colDef;
    }
    
    public void initializeReaders(){
        try {
            this.fr=new FileReader(this.filepath);
            this.bufr=new BufferedReader(fr);
            
        }
        catch (FileNotFoundException fe){
            System.out.println("File not found"+fe.getMessage());
        } 
    }
    
    public String readLine(){
        
        String line=null;
        if(this.fr==null||this.bufr==null){
            initializeReaders();
        }
        try{
            line=bufr.readLine();
            if(line==null){
                bufr.close();
                fr.close();
                bufr=null;
                fr=null;
            }
        }
        catch(IOException ioe){
            System.out.println("IO Exception"+ioe.getMessage());
        }
        return line;
        
    }
    
    //public HashMap<String,ColumnDefinition> getSchema(){
    public ArrayList<ColumnDefinition> getSchema(){
        return this.schema;
    }
    //Sets the schema as a Hash Map of Column Name, Column Data type
    
    public void setSchema(){
        
        for (ColumnDefinition cd : colDef){

            //schema.put(cd.getColumnName(), cd);
        	schema.add(cd);
            
        }
        
        
    }
    @Override
    public String toString(){
        tableToString = "Schema \n--------------\nTable Name : " + tableName + "\nNumber of Columns : " + numCol;
        for (ColumnDefinition cd : colDef){
            //System.out.println("Column Name : " + cd.getColumnName() + "Column data type : " + cd.getColDataType().getDataType());
            tableToString = tableToString.concat("\nColumn Name : " + cd.getColumnName() + "\nColumn data type : " + cd.getColDataType().getDataType());
            
        }
        return tableToString;
    }
    
}
            

