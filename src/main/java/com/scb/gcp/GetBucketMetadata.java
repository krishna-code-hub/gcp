package com.scb.gcp;

//import org.apache.log4j.LogManager;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;

import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;

public class GetBucketMetadata {

    static String  path = "C:\\Users\\Krishna\\Downloads\\gcs";

    public static void main(String[] args) {
        getBucketMetadata("sc-test-360609","sc-test-merklesg");

        //listBucket("sc-test-360609","sc-test-merklesg");

        //System.out.println(GetBucketMetadata.class.getName());
    }

    public static void listBucket(String projectId, String bucketName) {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Page<Bucket> buckets = storage.list();

        for (Bucket bucket : buckets.iterateAll()) {
            System.out.println(bucket.getName());

        }
    }

    public static void downloadObject(
            String projectId, String bucketName, String objectName, String destFilePath) {
        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of your GCS bucket
        // String bucketName = "your-unique-bucket-name";

        // The ID of your GCS object
        // String objectName = "your-object-name";

        // The path to which the file should be downloaded
        // String destFilePath = "/local/path/to/file.txt";

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        Blob blob = storage.get(BlobId.of(bucketName, objectName));
        blob.downloadTo(Paths.get(destFilePath + "/" + objectName ));
        //blob.

        System.out.println(
                "Downloaded object "
                        + objectName
                        + " from bucket name "
                        + bucketName
                        + " to "
                        + destFilePath);
    }

    public static void getBucketMetadata(String projectId, String bucketName) {
        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of your GCS bucket
        // String bucketName = "your-unique-bucket-name";

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        // Select all fields. Fields can be selected individually e.g. Storage.BucketField.NAME
        Bucket bucket =
                storage.get(bucketName, Storage.BucketGetOption.fields(Storage.BucketField.values()));

        for (Iterator<Blob> it = bucket.list().getValues().iterator(); it.hasNext(); ) {
            Blob blb = it.next();
            System.out.println("Listing bucket content: " + blb.getName());
            downloadObject(projectId,bucketName,blb.getName(),path);
        }

        //System.out.println("Listing bucket content: " + bucket.list().getValues().iterator());
        // Print bucket metadata
        /*
        System.out.println("BucketName: " + bucket.getName());
        System.out.println("DefaultEventBasedHold: " + bucket.getDefaultEventBasedHold());
        System.out.println("DefaultKmsKeyName: " + bucket.getDefaultKmsKeyName());
        System.out.println("Id: " + bucket.getGeneratedId());
        System.out.println("IndexPage: " + bucket.getIndexPage());
        System.out.println("Location: " + bucket.getLocation());
        System.out.println("LocationType: " + bucket.getLocationType());
        System.out.println("Metageneration: " + bucket.getMetageneration());
        System.out.println("NotFoundPage: " + bucket.getNotFoundPage());
        System.out.println("RetentionEffectiveTime: " + bucket.getRetentionEffectiveTime());
        System.out.println("RetentionPeriod: " + bucket.getRetentionPeriod());
        System.out.println("RetentionPolicyIsLocked: " + bucket.retentionPolicyIsLocked());
        System.out.println("RequesterPays: " + bucket.requesterPays());
        System.out.println("SelfLink: " + bucket.getSelfLink());
        System.out.println("StorageClass: " + bucket.getStorageClass().name());
        System.out.println("TimeCreated: " + bucket.getCreateTime());
        System.out.println("VersioningEnabled: " + bucket.versioningEnabled());
        if (bucket.getLabels() != null) {
            System.out.println("\n\n\nLabels:");
            for (Map.Entry<String, String> label : bucket.getLabels().entrySet()) {
                System.out.println(label.getKey() + "=" + label.getValue());
            }
        }
        if (bucket.getLifecycleRules() != null) {
            System.out.println("\n\n\nLifecycle Rules:");
            for (BucketInfo.LifecycleRule rule : bucket.getLifecycleRules()) {
                System.out.println(rule);
            }
        }

         */
    }
}
// [END storage_get_bucket_metadata]

