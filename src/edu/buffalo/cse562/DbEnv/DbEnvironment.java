package edu.buffalo.cse562.DbEnv;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;

public class DbEnvironment {
	private Environment myEnv;
    private Database vendorDb;
    private Database inventoryDb;

    public DbEnvironment() {} 
    public void setup(File envHome, boolean readOnly)
            throws DatabaseException {

        // Instantiate an environment and database configuration object
        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        DatabaseConfig myDbConfig = new DatabaseConfig();
        // Configure the environment and databases for the read-only
        // state as identified by the readOnly parameter on this 
        // method call.
        myEnvConfig.setReadOnly(readOnly);
        myDbConfig.setReadOnly(readOnly);
        // If the environment is opened for write, then we want to be
        // able to create the environment and databases if 
        // they do not exist.
        myEnvConfig.setAllowCreate(!readOnly);
        myDbConfig.setAllowCreate(!readOnly);

        // Instantiate the Environment. This opens it and also possibly
        // creates it.
        myEnv = new Environment(envHome, myEnvConfig);

        // Now create and open our databases.
        vendorDb = myEnv.openDatabase(null,
                                       "VendorDB",
                                       myDbConfig); 

        inventoryDb = myEnv.openDatabase(null,
                                         "InventoryDB",
                                         myDbConfig);
    }  
    public Environment getEnvironment() {
        return myEnv;
    }

    public Database getVendorDB() {
        return vendorDb;
    }

    public Database getInventoryDB() {
        return inventoryDb;
    } 

    public void close() {
        if (myEnv != null) {
            try {
                vendorDb.close();
                inventoryDb.close();
                myEnv.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing MyDbEnv: " + 
                                    dbe.toString());
                System.exit(-1);
            }
        }
    }
}
