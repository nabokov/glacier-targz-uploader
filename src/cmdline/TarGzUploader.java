package cmdline;

import glacierHelper.*;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;

public class TarGzUploader {
	public static String endPoint = "https://glacier.us-east-1.amazonaws.com/";

	public static AmazonGlacierClient client;

	/**
	 * Creates tar.gz archive of the specified directory on the fly, and
	 * sends it to the specified glacier vault.
	 *
	 * (use PipedUploader in combination with "tar -cvzf - DIRECTORY |" instead, if
	 *  you cannot trust the code in TarGzStream.java...)
	 *
	 * example:
	 *   java -Xmx1G -Dfile.encoding=UTF-8 -jar tarGzUploader.jar VAULT_NAME DIRECTORY [ARCHIVE_DESCRIPTION]
	 */
	public static void main(String[] args) {
		// deal with command line args
		if (args.length < 2) {
			System.out.println("Please specify command line args: vault_name directory_to_upload [description]");
			System.exit(1);
		}
		String vaultName = args[0];
		String filePath = args[1];
		String archiveDescription = "";
		if (args.length >= 3) {
			archiveDescription = args[2];
		} else {
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
		System.out.println("Uploading all contents of directory "+filePath+" as tar.gz...");

		// read credentials
		try {
			AWSCredentials credentials = new PropertiesCredentials(
					TarGzUploader.class.getResourceAsStream("AwsCredentials.properties")
					);
			client = new AmazonGlacierClient(credentials);
			client.setEndpoint(endPoint);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

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
}
