package glacierHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Keeps contentRange and checkSum of each part in a 'bookmark' file during
 * multi-part upload, so that the upload can be resumed afterwards in case of
 * transient errors.
 */
public class PartialUploadStatus {
    static final String basePath = "./";

    private Properties status;
    private String fullFileName;

    /**
     * Prepares the 'bookmark' file where record of partial uploads will be kept
     */
    public PartialUploadStatus(String fileName) {
        fullFileName = basePath + "partialUploadStatus."+fileName+".properties";
        status = new Properties();

        FileInputStream in;
        try {
            in = new FileInputStream(fullFileName);
            status.load(in);
            in.close();
        } catch (FileNotFoundException e) {
            // no problem. A new upload.
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to load previous partial upload status from file:"+fullFileName);
            System.err.println("Will upload everything from the beginning");
        }
    }

    /**
     * Associates a single upload ID with the current record.
     * Separate PartialUploadStatus object and bookmark file should be used for
     * each upload IDs.
     */
    public void setUploadId(String uploadId) {
        status.setProperty("uploadId", uploadId);
    }

    public String getUploadId() {
        return status.getProperty("uploadId");
    }

    private String escapeKeyStr(String key) {
        return key.replaceAll("[=: ]+", "");
    }

    /**
     * records the successful partial upload in the bookmark file.
     */
    public void bookmarkSuccessfulUpload(String contentRange, String checkSum) {
        status.setProperty(escapeKeyStr(contentRange), checkSum);
        saveFile();
    }

    /**
     * checks if there is a record of successful upload identified by
     * contentRange and checkSum.
     */
    public boolean previouslyUploaded(String contentRange, String checkSum) {
        String previousChecksum = status.getProperty(escapeKeyStr(contentRange));
        if (previousChecksum == null) return false;

        if (previousChecksum.equals(checkSum)) {
            return true;
        } else {
            System.err.println("Warning:checksum does not match.");
            return false;
        }
    }

    /**
     * delete the bookmark file
     */
    public void deleteFile() {
        File file = new File(fullFileName);
        file.delete();
    }

    /**
     * Saves the current status in the bookmark file.
     */
    public void saveFile() {
        FileOutputStream out;
        try {
            out = new FileOutputStream(fullFileName);
            status.store(out, "Auto-generated partial upload status. Can be deleted safely.");
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.err.println("Failed to save partial upload status to file:"+fullFileName);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Failed to save partial upload status to file:"+fullFileName);
        }
    }
}
