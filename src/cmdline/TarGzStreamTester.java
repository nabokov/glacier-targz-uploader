package cmdline;
import glacierHelper.TarGzStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;

/**
 * Sends output of TarGzStream object to stdout.
 * (Merely for testing TarGzStream class).
 * 
 * example:
 *   java -Xmx1G -Dfile.encoding=UTF-8 -jar tarGzStreamTester.jar DIRECTORY > OUTPUT_TAR_GZ_FILENAME
 */
public class TarGzStreamTester {

	private static int bufferSize = 1024 * 1024;

	private InputStream pipedIn;

    public TarGzStreamTester(InputStream pipedIn) {
    	this.pipedIn = pipedIn;
    }

    public void startProcessingStream() throws IOException {
        byte[] buffer = new byte[bufferSize];

        while (true) {
        	int read = pipedIn.read(buffer, 0, buffer.length);
        	if (read == -1) break;

            byte[] bytesRead = Arrays.copyOf(buffer, read);
            System.out.write(bytesRead);
        }
        System.out.flush();
    }

	public static void main(String[] args)  {
		if (args.length < 1) {
			System.out.println("Please specify directory.");
			System.exit(1);
		}

		TarGzStream tarGz = new TarGzStream(args[0]);
		tarGz.setVerbose(false);

		PipedOutputStream tarGzStream = tarGz.prepareOutputStream();
		PipedInputStream pipeIn = new PipedInputStream();
		try {
			pipeIn.connect(tarGzStream);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}

		TarGzStreamTester reader = new TarGzStreamTester(pipeIn);
		Thread tarThread = new Thread(tarGz);
		tarThread.start();
		try {
			reader.startProcessingStream();
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(1);
		}

		try {
			tarThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
