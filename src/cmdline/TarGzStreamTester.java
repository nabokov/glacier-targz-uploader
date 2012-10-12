package cmdline;
import glacierHelper.TarGzStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;


/**
 * Sends output of TarGzStream object to stdout.
 * (Merely for testing TarGzStream class).
 * 
 * example:
 *   java -Xmx1G -Dfile.encoding=UTF-8 -jar tarGzStreamTester.jar -dir DIRECTORY > OUTPUT_TAR_GZ_FILENAME
 */
public class TarGzStreamTester {

    private static int bufferSize = 1024 * 1024;
    private static String filePath = null;

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
        // deal with command line args
        treatCommandlineArgs(args);

        TarGzStream tarGz = new TarGzStream(filePath);
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

    private static void treatCommandlineArgs(String args[]) {
        Options options = new Options();
        options.addOption("dir", true, "directory to process");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            filePath = cmd.getOptionValue("dir");
        } catch (ParseException e1) {
            e1.printStackTrace();
            System.exit(1);
        }

        // check mandatory options
        if (filePath == null) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -Dfile.encoding=UTF-8 -Xmx1G -jar tarGzStreamTester.jar", options, true);
            System.exit(0);
        }
    }
}
