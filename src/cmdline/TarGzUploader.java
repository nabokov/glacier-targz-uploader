package cmdline;

import glacierHelper.PartialUploadStatus;
import glacierHelper.StreamUploader;
import glacierHelper.TarGzStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;

public class TarGzUploader {
    private static String credentialsFilename = "AwsCredentials.properties";
    private static String region = "us-east-1";

    private static AmazonGlacierClient client;

    private static String vaultName = null;
    private static String filePath = null;
    private static String archiveDescription = null;
    private static String bookmarkFile = null;

    /**
     * Creates tar.gz archive of the specified directory on the fly, and
     * sends it to the specified glacier vault.
     *
     * (use PipedUploader in combination with "tar -cvzf - DIRECTORY |" instead, if
     *  you cannot trust the code in TarGzStream.java...)
     *
     * example:
     *   java -Xmx1G -Dfile.encoding=UTF-8 -jar tarGzUploader.jar -vault VAULT_NAME -dir DIRECTORY [-bookmark BOOKMARK_NAME] [-desc ARCHIVE_DESCRIPTION]
     */
    public static void main(String[] args) {
        // deal with command line args
        treatCommandlineArgs(args);

        System.out.println("Uploading all contents of directory "+filePath+" as tar.gz...");

        // prepare tar.gz stream
        TarGzStream tarGz = new TarGzStream(filePath);
        PipedOutputStream tarGzStream = tarGz.prepareOutputStream();
        PipedInputStream pipeIn = new PipedInputStream();
        try {
            pipeIn.connect(tarGzStream);
        } catch (IOException e) {
            System.out.println(e);
            System.exit(1);
        }

        // read and compress directory in one thread, upload in another thread.
        StreamUploader uploader = new StreamUploader(client, vaultName, archiveDescription, pipeIn);
        if (bookmarkFile != null) {
            uploader.setPartialUploadStatus(new PartialUploadStatus(bookmarkFile));
        }
        Thread tarThread = new Thread(tarGz);
        tarThread.start();
        String archiveId = uploader.startProcessingStream();

        System.out.println("Upload complete.");
        System.out.println("Archive ID:"+archiveId);

        // cleanup
        try {
            tarThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void treatCommandlineArgs(String args[]) {
        Options options = new Options();
        options.addOption("vault", true, "vault name");
        options.addOption("dir", true, "directory to upload");
        options.addOption("credentials", true, "AWS credentials file name (defaults to 'AwsCredentials.properties')");
        options.addOption("region", true, "AWS region (defaults to 'us-east-1')");
        options.addOption("desc", true, "(optional) archive description");
        options.addOption("bookmark", true, "(optional) bookmark name to keep track of partial uploads. Specify an unique name if you may wish to stop and resume this upload, or if you are resuming a previously aborted upload.");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            vaultName = cmd.getOptionValue("vault");
            filePath = cmd.getOptionValue("dir");
            archiveDescription = cmd.getOptionValue("desc");
            bookmarkFile = cmd.getOptionValue("bookmark");
            credentialsFilename = cmd.getOptionValue("credentials", credentialsFilename);
            region = cmd.getOptionValue("region", region);
        } catch (ParseException e1) {
            e1.printStackTrace();
            System.exit(1);
        }

        // check mandatory options
        if (vaultName == null || filePath == null) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -Dfile.encoding=UTF-8 -Xmx1G -jar tarGzUploader.jar", options, true);
            System.exit(0);
        }

        // set description
        if (archiveDescription == null) {
            try {
                archiveDescription = "Archive of "
                        // looks like archive name has to be in ascii (or check sum does not match during upload?)
                        + URLEncoder.encode(filePath, "UTF-8")
                        + ", "+(new Date());
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        // read credentials
        try {
            AWSCredentials credentials = new PropertiesCredentials(
                    new FileInputStream(credentialsFilename)
                    );
            client = new AmazonGlacierClient(credentials);
            client.setEndpoint("https://glacier."+region+".amazonaws.com/");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
