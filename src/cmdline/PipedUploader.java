package cmdline;

import glacierHelper.*;

import java.io.FileInputStream;
import java.io.IOException;
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

public class PipedUploader {
    private static String credentialsFilename = "AwsCredentials.properties";
    private static String region = "us-east-1";

    private static AmazonGlacierClient client;

    private static String vaultName = null;
    private static String archiveDescription = null;
    private static String bookmarkFile = null;

    /**
     * Sends whatever comes into stdin to the specified glacier vault.
     *
     * example:
     *   tar -cvzf - DIRECTORY | java -Xmx1G -Dfile.encoding=UTF-8 -jar pipedUploader.jar -vault VAULE_NAME [-bookmark BOOKMARK_NAME] [-desc ARCHIVE_DESCRIPTION]
     * 
     */
    public static void main(String[] args) {
        // deal with command line args
        treatCommandlineArgs(args);

        System.out.println("Sending stdin to the vault "+vaultName+"...");

        // read stdin and send it to the vault
        StreamUploader uploader = new StreamUploader(client, vaultName, archiveDescription, System.in);
        if (bookmarkFile != null) {
            uploader.setPartialUploadStatus(new PartialUploadStatus(bookmarkFile));
        }
        String archiveId = uploader.startProcessingStream();

        System.out.println("Upload complete.");
        System.out.println("Archive ID:"+archiveId);
    }

    private static void treatCommandlineArgs(String args[]) {
        Options options = new Options();
        options.addOption("vault", true, "vault name");
        options.addOption("credentials", true, "AWS credentials file name (defaults to 'AwsCredentials.properties')");
        options.addOption("region", true, "AWS region (defaults to 'us-east-1')");
        options.addOption("desc", true, "(optional) archive description");
        options.addOption("bookmark", true, "(optional) bookmark name to keep track of partial uploads. Specify an unique name if you may wish to stop and resume this upload, or if you are resuming a previously aborted upload.");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            vaultName = cmd.getOptionValue("vault");
            archiveDescription = cmd.getOptionValue("desc");
            bookmarkFile = cmd.getOptionValue("bookmark");
            credentialsFilename = cmd.getOptionValue("credentials", credentialsFilename);
            region = cmd.getOptionValue("region", region);
        } catch (ParseException e1) {
            e1.printStackTrace();
            System.exit(1);
        }

        // check mandatory options
        if (vaultName == null) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -Dfile.encoding=UTF-8 -Xmx1G -jar pipedUploader.jar", options, true);
            System.exit(0);
        }

        // set description
        if (archiveDescription == null) {
            archiveDescription = "Archive created on "+(new Date());
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
