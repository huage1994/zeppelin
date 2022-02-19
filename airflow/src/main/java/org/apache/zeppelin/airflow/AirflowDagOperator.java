package org.apache.zeppelin.airflow;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_NOTEBOOK_OSS_BUCKET;
import static org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars.ZEPPELIN_OSS_REMOTE_BASE_FOLDER;

public abstract class AirflowDagOperator extends AbstractInterpreter {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AirflowDagOperator.class);
    protected String bucketName;
    protected String ossRemoteBaseFolder;

    public AirflowDagOperator(Properties properties) {
        super(properties);
        bucketName = this.getProperty(ZEPPELIN_NOTEBOOK_OSS_BUCKET.getVarName());
        LOGGER.info(bucketName);
        ossRemoteBaseFolder = this.getProperty(ZEPPELIN_OSS_REMOTE_BASE_FOLDER.getVarName());
        ossRemoteBaseFolder = ossRemoteBaseFolder.substring(ossRemoteBaseFolder.indexOf(bucketName) + bucketName.length());
        if (ossRemoteBaseFolder.startsWith("/")) {
            ossRemoteBaseFolder = ossRemoteBaseFolder.substring(1);
        }
    }


    @Override
    public ZeppelinContext getZeppelinContext() {
        return null;
    }

    @Override
    protected InterpreterResult internalInterpret(String st, InterpreterContext context) throws InterpreterException {
        LOGGER.info("interpret");

        InterpreterResult interpreterResult = dagOperate(st, context);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.info("Exception when sleep", e);
            throw new InterpreterException(e);
        }
        return interpreterResult;
    }

    protected abstract InterpreterResult dagOperate(String st, InterpreterContext context);

    protected String buildDagFileKey(InterpreterContext context) {
        String fileName = context.getNoteName() + "_" + context.getNoteId() + "_" + context.getParagraphId() + ".py";
        String key = "airflow/dags/zeppelin_dags/" + fileName;
        if (StringUtils.isNotEmpty(ossRemoteBaseFolder)) {
            key = ossRemoteBaseFolder + "/" + key;
        }
        LOGGER.info(key);
        return key;
    }

    @Override
    public void open() throws InterpreterException {

    }

    @Override
    public void cancel(InterpreterContext context) throws InterpreterException {

    }

    @Override
    public FormType getFormType() throws InterpreterException {
        return FormType.NATIVE;
    }

    @Override
    public int getProgress(InterpreterContext context) throws InterpreterException {
        return 0;
    }
}
