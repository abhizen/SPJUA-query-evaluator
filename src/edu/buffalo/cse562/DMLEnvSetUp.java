package edu.buffalo.cse562;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by abhinit on 9/20/15.
 */
public class DMLEnvSetUp {


    private EnvironmentConfig envConfig = null;

    public DMLEnvSetUp(){
        envConfig = new EnvironmentConfig();
        envSetUp();
        setLogConfig();
    }

    public EnvironmentConfig getEnvConfig() {
        return envConfig;
    }

    private void envSetUp(){
        envConfig.setAllowCreate(true);
        envConfig.setConfigParam(EnvironmentConfig.LOG_CHECKSUM_READ, "false");
        envConfig.setConfigParam(EnvironmentConfig.LOG_VERIFY_CHECKSUMS,"false");
        envConfig.setConfigParam(EnvironmentConfig.STATS_COLLECT,"false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER,"false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER,"false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_IN_COMPRESSOR,"false");
        envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR,"false");
        envConfig.setConfigParam(EnvironmentConfig.LOG_MEM_ONLY,"true");
        envConfig.setLocking(false);
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, "INFO");
    }

    private void setLogConfig(){
        /*Setting log  configuration*/
        Logger parent = Logger.getLogger("com.sleepycat.je");
        parent.setLevel(Level.INFO);
        envConfig.setConfigParam(EnvironmentConfig.FILE_LOGGING_LEVEL, "INFO");
    }

}
