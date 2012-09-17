package cmdline;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;

/**
 * Downloads archive from specified vault to specified file.
 *
 * Code mostly copied & pasted from: http://awsdocs.s3.amazonaws.com/glacier/latest/glacier-dg.pdf
 *
 */
public class Downloader {
	public static String endPoint = "https://glacier.us-east-1.amazonaws.com/";
    public static AmazonGlacierClient client;

    public static void main(String[] args) throws IOException {
    	if (args.length != 3) {
    		System.out.println("Please specify command line args: vault_name archiveId download_filename");
    		System.exit(1);
    	}

    	String vaultName = args[0];
    	String archiveId = args[1];
    	String downloadFilePath = args[2];
    	System.out.println("Downloading from vault "+vaultName+" to file "+downloadFilePath);
    	System.out.println("Download may take hours; please be patient...");

    	AWSCredentials credentials = new PropertiesCredentials(
                Downloader.class.getResourceAsStream("AwsCredentials.properties"));
        client = new AmazonGlacierClient(credentials);
        client.setEndpoint(endPoint);

        try {
            ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
            atm.download(vaultName, archiveId, new File(downloadFilePath));
        } catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Download complete.");
    }

}