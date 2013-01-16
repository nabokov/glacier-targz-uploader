package cmdline;
import glacierHelper.VaultInventory;

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

public class ListVaultInventory {
    private static String credentialsFilename = "AwsCredentials.properties";
    private static String region = "us-east-1";

    private static AWSCredentials credentials;

    private static String vaultName = null;


    public static void main(String[] args) {
        // deal with command line args
        treatCommandlineArgs(args);

        try {
            VaultInventory inventory = new VaultInventory(region, vaultName, credentials);
            System.out.println(inventory.getResponseString());
        } catch (IOException e) {
        }
    }

    private static void treatCommandlineArgs(String args[]) {
        Options options = new Options();
        options.addOption("vault", true, "vault name");
        options.addOption("credentials", true, "AWS credentials file name (defaults to 'AwsCredentials.properties')");
        options.addOption("region", true, "AWS region (defaults to 'us-east-1')");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            vaultName = cmd.getOptionValue("vault");
            credentialsFilename = cmd.getOptionValue("credentials", credentialsFilename);
            region = cmd.getOptionValue("region", region);
        } catch (ParseException e1) {
            e1.printStackTrace();
            System.exit(1);
        }

        // check mandatory options
        if (vaultName == null) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -Dfile.encoding=UTF-8 -Xmx1G -jar listVaultInventory.jar", options, true);
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