package it.uniroma2.sae.config;

/**
 * Configuration for Flink checkpoint storage backend.
 * Controlled via the {@code flink.checkpoint} section of application.yaml.
 *
 * <p>Supported storage types:
 * <ul>
 *   <li>{@code "hdfs"} — saves checkpoints to HDFS (local Docker Compose setup)</li>
 *   <li>{@code "s3"}   — saves checkpoints to Amazon S3 (AWS EC2 deployment)</li>
 *   <li>{@code "local"} — saves checkpoints to a local filesystem path</li>
 *   <li>{@code null}/empty — no explicit checkpoint storage configured (Flink default)</li>
 * </ul>
 */
public class CheckpointStorageConfig {

    /** Storage backend type: "hdfs", "s3", "local", or null/empty for Flink default. */
    private String storageType;

    /**
     * Checkpoint directory path.
     * <ul>
     *   <li>For HDFS: path inside the HDFS namespace, e.g. {@code /flink/checkpoints}</li>
     *   <li>For S3:   path inside the bucket, e.g. {@code flink/checkpoints}</li>
     *   <li>For local: absolute filesystem path, e.g. {@code /tmp/flink-checkpoints}</li>
     * </ul>
     */
    private String path;

    /** HDFS-specific configuration, used when storageType = "hdfs". */
    private HdfsConfig hdfs;

    /** S3-specific configuration, used when storageType = "s3". */
    private S3Config s3;

    // --- Getters & Setters ---

    public String getStorageType() { return storageType; }
    public void setStorageType(String storageType) { this.storageType = storageType; }

    public String getPath() { return path; }
    public void setPath(String path) { this.path = path; }

    public HdfsConfig getHdfs() { return hdfs; }
    public void setHdfs(HdfsConfig hdfs) { this.hdfs = hdfs; }

    public S3Config getS3() { return s3; }
    public void setS3(S3Config s3) { this.s3 = s3; }

    // =========================================================
    // Inner config classes
    // =========================================================

    /**
     * HDFS connection settings.
     */
    public static class HdfsConfig {

        /**
         * HDFS NameNode URI, e.g. {@code hdfs://hdfs.flight-analysis.local:54310}.
         * This is used as {@code fs.defaultFS} in the Hadoop configuration.
         */
        private String namenode;

        public String getNamenode() { return namenode; }
        public void setNamenode(String namenode) { this.namenode = namenode; }
    }

    /**
     * Amazon S3 connection settings.
     * When running on EC2 with an IAM Instance Profile, leave
     * {@code accessKey} and {@code secretKey} empty — the AWS SDK will
     * pick up the credentials automatically from the instance metadata service.
     */
    public static class S3Config {

        /** S3 bucket name (without the {@code s3://} prefix). */
        private String bucket;

        /** AWS region where the bucket lives, e.g. {@code us-east-1}. */
        private String region;

        /**
         * AWS Access Key ID.
         * Leave empty when using IAM Instance Profile credentials on EC2.
         */
        private String accessKey = "";

        /**
         * AWS Secret Access Key.
         * Leave empty when using IAM Instance Profile credentials on EC2.
         */
        private String secretKey = "";

        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }

        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }

        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }

        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
    }
}
