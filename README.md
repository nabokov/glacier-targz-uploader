glacier-targz-uploader
======================

(日本語の解説はこのブログ記事をどうぞ: http://blog.livedoor.jp/nabokov7/archives/1517648.html )

What is this ?
--------------

Set of command line tools to archive/retrieve your data to/from Amazon Glacier vault.
- Can upload a large directory as tar.gz without creating an intermediate file.
- Can resume upload after interrupt / transient failure.

Currently, following 4 tools are available:
- TarGzUploader: Can compress and upload all contents of the specified directory without creating an intermediate tar.gz file.
- StreamUploader: Can send whatever comes into stdin to the vault.
- Downloader: Downloads the specified archive.
- ListInventory: Makes list of inventories in the vault. (Just a copy-paste of the code in the official manual...)

Instructions
------------

- Create your glacier vault in aws management console, set your credentials in AWSCredentials.properties, and then:

Either

- Run the executable jar files in builds/ directory.

  (ex.)  java -Xmx1G -Dfile.encoding=UTF-8 -jar listVaultInventory.jar -vault [vault_name]

Or...

- Compile and create executable jars for each of the classes under src/cmdline.

For usage, run the executable jar without args. Usages in more detail are embedded as comments.
Remember to set your own AWS credentials in AWSCredentials.properties file.

Prerequisites (to compile the code)
-------------

- Works best with AWS Toolkit for Eclipse (http://aws.amazon.com/eclipse/). Otherwise, you will have to download AWS SDK that comes with the toolkit and setup your library paths accordingly.

- You will also need commons-compress-1.4.1.jar (http://commons.apache.org/compress/download_compress.cgi ) and commons-cli-1.2.jar (http://commons.apache.org/cli/download_cli.cgi) 

