package org.apache.zeppelin.notebook.repo;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.*;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class OSSOperator {
    private OSS ossClient;

    public OSSOperator(OSS ossClient) {
        this.ossClient = ossClient;
    }


    public String getTextObject(String bucketName, String key) throws IOException {
        OSSObject ossObject = ossClient.getObject(bucketName, key);
        InputStream in = null;
        try {
            in = ossObject.getObjectContent();
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }


    public void putTextObject(String bucketName, String key, InputStream inputStream) {
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, inputStream);
        ossClient.putObject(putObjectRequest);
    }


    public void moveObject(String bucketName, String sourceKey, String destKey) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucketName,
                sourceKey, bucketName, destKey);
        ossClient.copyObject(copyObjectRequest);
        ossClient.deleteObject(bucketName, sourceKey);
    }

    public void moveDir(String bucketName, String sourceDir, String destDir) {
        List<String> objectKeys = listDirObjects(bucketName, sourceDir);
        for (String key : objectKeys) {
            moveObject(bucketName, key, destDir + key.substring(sourceDir.length()));
        }
    }


    public void deleteDir(String bucketName, String dirname) {
        List<String> keys = listDirObjects(bucketName, dirname);
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(keys);
        ossClient.deleteObjects(deleteObjectsRequest);
    }

    public void deleteFiles(String bucketName, List<String> objectKeys) {
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName).withKeys(objectKeys);
        ossClient.deleteObjects(deleteObjectsRequest);
    }


    public List<String> listDirObjects(String bucketName, String dirname) {
        String nextMarker = null;
        ObjectListing objectListing = null;
        List<String> keys = new ArrayList<>();
        do {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName)
                    .withPrefix(dirname)
                    .withMarker(nextMarker);
            objectListing = ossClient.listObjects(listObjectsRequest);
            if (!objectListing.getObjectSummaries().isEmpty()) {
                for (OSSObjectSummary s : objectListing.getObjectSummaries()) {
                    keys.add(s.getKey());
                }
            }

            nextMarker = objectListing.getNextMarker();
        } while (objectListing.isTruncated());
        return keys;
    }


}
