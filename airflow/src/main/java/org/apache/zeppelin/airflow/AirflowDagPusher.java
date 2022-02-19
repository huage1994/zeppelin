package org.apache.zeppelin.airflow;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.shell.ShellInterpreter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class AirflowDagPusher extends AirflowDagOperator {

    private ShellInterpreter shellInterpreter;

    public AirflowDagPusher(Properties properties) {
        super(properties);
        shellInterpreter = new ShellInterpreter(properties);
    }


    @Override
    public void open() throws InterpreterException {
        shellInterpreter.open();
    }

    @Override
    public void cancel(InterpreterContext context) throws InterpreterException {
        shellInterpreter.cancel(context);
    }

    @Override
    public void close() throws InterpreterException {
        shellInterpreter.close();
        super.close();
    }


    @Override
    protected InterpreterResult dagOperate(String st, InterpreterContext context) {
        String key = buildDagFileKey(context);

        try {
            // write dag to tmp file , use python under conda env airflow to run tmp file to verify dag.
            String tmpDagPath = Files.createTempDirectory(
                    context.getNoteId()).toAbsolutePath().toString() + key;
            writeTextToFile(st, tmpDagPath);
            InterpreterResult interpreterResult = shellInterpreter.internalInterpret("source activate airflow && python " + tmpDagPath, context);
            if (interpreterResult.code().equals(InterpreterResult.Code.ERROR)) {
                return interpreterResult;
            }
            // push dag to airflow dag dir
            String dagFilePath = "/" + bucketName + "/" + key;
            writeTextToFile(st, dagFilePath);
            interpreterResult.add("\nDAG has been pushed to airlfow");
            return interpreterResult;

        } catch (IOException e) {
            return new InterpreterResult(InterpreterResult.Code.ERROR, "Exception when write file " + " ,Exception is " + e);
        }
    }


    public void writeTextToFile(String st, String dest) throws IOException {
        File file = new File(dest);
        file.getParentFile().mkdirs();

        if (file.exists() && !file.delete()) {
            LOGGER.error("Failed to delete existed file when update dag");
        }
        LOGGER.info("Push to dagFilePath: ", dest);
        try (FileWriter writer = new FileWriter(dest)) {
            writer.write(st);
        }
    }
}
