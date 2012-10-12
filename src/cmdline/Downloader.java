package cmdline;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;

/**
 * Downloads archive from specified vault to specified file.
 *
 * example:
 *   java -Xmx1G -Dfile.encoding=UTF-8 -jar downloader.jar -vault VAULT_NAME -archive_id ARCHIVE_ID-out_file FILENAME
 *
 * Code mostly copied & pasted from: http://awsdocs.s3.amazonaws.com/glacier/latest/glacier-dg.pdf
 *
 */
public class Downloader {
    private static String credentialsFilename = "AwsCredentials.properties";
    private static String region = "us-east-1";

    private static AWSCredentials credentials;
    private static AmazonGlacierClient client;
    private static ArchiveTransferManager atm = null;

    private static String vaultName = null;
    private static String archiveId = null;
    private static String downloadFilePath = null;

    public static void main(String[] args) throws IOException {
        // deal with command line args
        treatCommandlineArgs(args);

        System.out.println("Downloading from vault "+vaultName+" to file "+downloadFilePath);
        System.out.println("Download may take hours; please be patient...");

        try {
            client = new AmazonGlacierClient(credentials);
            client.setEndpoint("https://glacier."+region+".amazonaws.com/");
            atm = new ArchiveTransferManager(credentials);
            atm.download(vaultName, archiveId, new File(downloadFilePath));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Download complete.");
    }

    private static void treatCommandlineArgs(String args[]) {
        Options options = new Options();
        options.addOption("vault", true, "vault name");
        options.addOption("archive_id", true, "archive ID");
        options.addOption("out_file", true, "filename to which the downloaded content is written");
        options.addOption("credentials", true, "AWS credentials file name (defaults to 'AwsCredentials.properties')");
        options.addOption("region", true, "AWS region (defaults to 'us-east-1')");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            vaultName = cmd.getOptionValue("vault");
            archiveId = cmd.getOptionValue("archive_id");
            downloadFilePath = cmd.getOptionValue("out_file");
            credentialsFilename = cmd.getOptionValue("credentials", credentialsFilename);
            region = cmd.getOptionValue("region", region);
        } catch (ParseException e1) {
            e1.printStackTrace();
            System.exit(1);
        }

        // check mandatory options
        if (vaultName == null || archiveId == null || downloadFilePath == null) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -Dfile.encoding=UTF-8 -Xmx1G -jar downloader.jar", options, true);
            System.exit(0);
        }

        // read credentials
        try {
            credentials = new PropertiesCredentials(
                    new FileInputStream(credentialsFilename)
                    );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}