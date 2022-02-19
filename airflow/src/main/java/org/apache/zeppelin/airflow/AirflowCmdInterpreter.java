package org.apache.zeppelin.airflow;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.shell.ShellInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_OSS_REMOTE_BASE_FOLDER;

public class AirflowCmdInterpreter extends ShellInterpreter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AirflowCmdInterpreter.class);
    private static String ossRemoteBaseFolder;


    public AirflowCmdInterpreter(Properties property) {
        super(property);
        ossRemoteBaseFolder = this.getProperty(ZEPPELIN_OSS_REMOTE_BASE_FOLDER.getVarName());
    }

    @Override
    public void open() {
        super.open();
    }

    @Override
    public InterpreterResult internalInterpret(String cmd, InterpreterContext context) {
        String airflowCommand = "source activate airflow && " + "airflow " + cmd.trim();
        LOGGER.info("Airflow command: " + airflowCommand);
        return super.internalInterpret(airflowCommand, context);
    }

}
