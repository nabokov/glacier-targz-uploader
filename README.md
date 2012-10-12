glacier-targz-uploader
======================

(still in work)

What is this ?:

- Command line tools to archive/retrieve your data to/from Amazon Glacier vault.
- TarGzUploader: Can compress and upload all contents of the specified directory without creating an intermediate tar.gz file.
- StreamUploader: Can send whatever comes into stdin to the vault.
- Downloader: Downloads the specified archive.
- ListInventory: Makes list of inventories in the vault. (Just a copy-paste of the code in the official manual...)

Prerequisites:

- Works best with AWS Toolkit for Eclipse (http://aws.amazon.com/eclipse/). Otherwise, you will have to download AWS SDK that comes with the toolkit and setup your environment manually.

- You will also need commons-compress-1.4.1.jar (http://commons.apache.org/compress/download_compress.cgi ) and commons-cli-1.2.jar (http://commons.apache.org/cli/download_cli.cgi) 

Instructions:

- Edit AWSCredentials.properties.
- Compile and create executable jars for each of the classes under src/cmdline. For usage, run the executable jar without args. Usages in more detail are embedded as comments.
