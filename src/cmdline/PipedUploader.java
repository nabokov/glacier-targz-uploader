package cmdline;

import glacierHelper.*;

import java.io.IOException;
import java.util.Date;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;

public class PipedUploader {
	public static String endPoint = "https://glacier.us-east-1.amazonaws.com/";

	public static AmazonGlacierClient client;

	/**
	 * Sends whatever comes into stdin to the specified glacier vault.
	 *
	 * example:
	 *   tar -cvzf - DIRECTORY | java -Xmx1G -Dfile.encoding=UTF-8 -jar pipedUploader.jar VAULE_NAME [ARCHIVE_DISCRIPTION]
	 * 
	 */
	public static void main(String[] args) {
		// deal with command line args
		if (args.length < 1) {
			System.out.println("Please specify command line args: vault_name [description]");
			System.exit(1);
		}
		String vaultName = args[0];
		String archiveDescription = "";
		if (args.length >= 2) {
			archiveDescription = args[1];
		} else {
			archiveDescription = "Archive on "+(new Date());
		}
		System.out.println("Sending stdin to the vault "+vaultName+"...");

		// read credentials
		try {
			AWSCredentials credentials = new PropertiesCredentials(
					PipedUploader.class.getResourceAsStream("AwsCredentials.properties")
					);
			client = new AmazonGlacierClient(credentials);
			client.setEndpoint(endPoint);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// read stdin and send it to the vault
		StreamUploader uploader = new StreamUploader(client, vaultName, archiveDescription, System.in);
		String archiveId = uploader.startProcessingStream();

		System.out.println("Upload complete.");
		System.out.println("Archive ID:"+archiveId);
	}
}
