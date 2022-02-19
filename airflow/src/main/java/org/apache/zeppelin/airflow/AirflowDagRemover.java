package org.apache.zeppelin.airflow;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.io.File;
import java.util.Properties;

public class AirflowDagRemover extends AirflowDagOperator {

    public AirflowDagRemover(Properties properties) {
        super(properties);
    }


    @Override
    protected InterpreterResult dagOperate(String st, InterpreterContext context) {
        String key = buildDagFileKey(context);

        String dagFilePath = "/" + bucketName + "/" + key;
        LOGGER.info("Delete dagFilePath: ", dagFilePath);

        File file = new File(dagFilePath);

        if (file.delete()) {
            return new InterpreterResult(InterpreterResult.Code.SUCCESS, "DAG has been removed from airlfow");
        } else {
            return new InterpreterResult(InterpreterResult.Code.ERROR, "Failed to remove DAG from airlfow");
        }
    }
}
