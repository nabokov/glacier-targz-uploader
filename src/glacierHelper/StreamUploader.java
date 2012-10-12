package glacierHelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
//import com.amazonaws.services.glacier.model.AbortMultipartUploadRequest;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadResult;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadResult;
import com.amazonaws.services.glacier.model.UploadMultipartPartRequest;
import com.amazonaws.services.glacier.model.UploadMultipartPartResult;
import com.amazonaws.util.BinaryUtils;

/**
 * Reads content from the given inputStream and uploads it to the glacier vault
 * in multiple parts.
 * 
 * Some portion of this code taken from: http://docs.amazonwebservices.com/amazonglacier/latest/dev/uploading-an-archive-mpu-using-java.html#uploading-an-archive-in-parts-highlevel-using-java
 */

public class StreamUploader {

    // This example works for part sizes up to 1 GB.
    //public static String partSize = "1048576"; // 1MB
    private String partSize = "134217728"; // 128MB
    private boolean verbose = true;

    private String vaultName;
    private String archiveDescription;
    private AmazonGlacierClient client;
    private InputStream pipedIn;
    private String totalChecksum;
    private String totalLength;

    // setting valid partialUploadStatus objectwill enable resuming
    private PartialUploadStatus partialUploadStatus = null;

    /**
     * @param client
     * @param vaultName
     * @param archiveDescription
     * @param inputStream from which the content to be uploaded is read.
     */
    public StreamUploader(AmazonGlacierClient client, String vaultName, String archiveDescription, InputStream pipedIn) {
        this.client = client;
        this.vaultName = vaultName;
        this.archiveDescription = archiveDescription;
        this.pipedIn = pipedIn;

        this.totalChecksum = null;
        this.totalLength = null;
    }

    public void setVerbose(boolean verbose) { this.verbose = verbose; }
    public void setPartSize(long partSize) { this.partSize = String.valueOf(partSize); }

    public void setPartialUploadStatus(PartialUploadStatus partialUploadStatus) {
        this.partialUploadStatus = partialUploadStatus;
    }

    /**
     * Start processing input, and upload it in multiple parts.
     * 
     * @return archiveId on successful upload
     */
    public String startProcessingStream() {
        String archiveId = null;
        try {
            if (verbose) System.out.println("Uploading an archive.");
            String uploadId = initiateMultipartUpload();
            uploadParts(uploadId);
            CompleteMultipartUploadResult result = completeMultiPartUpload(uploadId);
            archiveId = result.getArchiveId();
            if (verbose) {
                System.out.println("Completed an archive.");
                System.out.println("Location:" + result.getLocation());
                System.out.println("Archive ID:" + archiveId);
                System.out.println("Total size: "+totalLength);
                System.out.printf("  (%2.2f GB)\n", (Float.valueOf(totalLength)/(1024*1024*1024)) );
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return archiveId;
    }

    private String initiateMultipartUpload() {
        String uploadId = null;
        if (partialUploadStatus != null) {
            uploadId = partialUploadStatus.getUploadId(); 
            if (uploadId != null) {
                if (verbose) System.out.println("Resuming previous upload:"+uploadId);
                return uploadId;
            }
        }

        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest()
        .withVaultName(vaultName)
        .withArchiveDescription(archiveDescription)
        .withPartSize(partSize);

        InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
        uploadId = result.getUploadId();

        if (verbose) System.out.println("uploadID: " + uploadId);
        if (partialUploadStatus != null) partialUploadStatus.setUploadId(uploadId);

        return uploadId;
    }

    private void uploadParts(String uploadId) throws AmazonServiceException, NoSuchAlgorithmException, AmazonClientException, IOException {
        byte[] buffer = new byte[Integer.valueOf(partSize)];
        List<byte[]> binaryChecksums = new LinkedList<byte[]>();

        long currentPosition = 0;
        String contentRange;

        TOTALLOOP: while (true) {
            // try to read exactly [partSize] of the input, unless it is the very last portion
            int read = 0;
            PARTIALCONTENTLOOP: while (read < buffer.length) {
                int subReadCount = pipedIn.read(buffer, read, buffer.length - read);
                if (subReadCount == -1) {
                    if (read == 0) { break TOTALLOOP; } // read only EOF
                    else { break PARTIALCONTENTLOOP; } // read last portion, less than [partSize]
                } else {
                    read += subReadCount;
                }
            }

            byte[] bytesRead = Arrays.copyOf(buffer, read);

            contentRange = String.format("bytes %s-%s/*", currentPosition, currentPosition + read - 1);
            String partialChecksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytesRead));
            byte[] binaryChecksum = BinaryUtils.fromHex(partialChecksum);
            binaryChecksums.add(binaryChecksum);
            if (verbose) System.out.println(contentRange);

            if (partialUploadStatus != null
                    && partialUploadStatus.previouslyUploaded(contentRange, partialChecksum)) {
                if (verbose)
                    System.out.println("Already uploaded successfully in the previous attempt. Skipping..");
            } else {
                //Upload part.
                UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
                .withVaultName(vaultName)
                .withBody(new ByteArrayInputStream(bytesRead))
                .withChecksum(partialChecksum)
                .withRange(contentRange)
                .withUploadId(uploadId);

                UploadMultipartPartResult partResult = client.uploadMultipartPart(partRequest);
                if (verbose) {
                    System.out.println("Part uploaded, checksum: " + partResult.getChecksum());
                    System.out.printf("Sent so far: %2.2f GB\n", ((float)currentPosition/(1024*1024*1024)));
                }

                if (partialUploadStatus != null)
                    partialUploadStatus.bookmarkSuccessfulUpload(contentRange, partialChecksum);
            }

            currentPosition = currentPosition + read;
        }
        pipedIn.close();
        totalChecksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
        totalLength = String.valueOf(currentPosition);
    }

    private CompleteMultipartUploadResult completeMultiPartUpload(String uploadId) throws NoSuchAlgorithmException, IOException {

        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
        .withVaultName(vaultName)
        .withUploadId(uploadId)
        .withChecksum(totalChecksum)
        .withArchiveSize(totalLength);

        if (partialUploadStatus != null) partialUploadStatus.deleteFile();

        return client.completeMultipartUpload(compRequest);
    }
}
