package org.dice_group.lod.down;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDownloader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDownloader.class);

    private static final String ENDPOINT = "http://sparql.backend.lodlaundromat.org/";

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        ListRequester requester = null;
        PrintStream metaOut = null;
        PrintStream curlScript = null;
        int count = 0;
        String timestamp = Long.toString(System.currentTimeMillis());
        try {
            requester = ListRequester.create(ENDPOINT);
            if (requester == null) {
                LOGGER.error("Couldn't request dataset meta data. Aborting.");
                return;
            }
            metaOut = new PrintStream(new File(timestamp + "-meta.tsv"), "UTF-8");
            metaOut.println("datadoc\tmd5\turl\ttriples\tparent");
            curlScript = new PrintStream(new File(timestamp + "-download.sh"), "UTF-8");
            while (requester.hasNext()) {
                DatasetMetaData meta = requester.next();
                writeLine(meta, metaOut);
                curlScript.print("curl -s -S -O http://download.lodlaundromat.org/");
                curlScript.print(meta.md5);
                curlScript.println("?type=hdt");
                ++count;
                if ((count % 1000) == 0) {
                    System.out.println("Saw " + count + ". dataset.");
                }
            }
            System.out.println("Done after " + count + " datasets.");
        } finally {
            IOUtils.closeQuietly(requester, metaOut);
        }
    }

    private static void writeLine(DatasetMetaData meta, PrintStream metaOut) {
        metaOut.print(meta.datadoc);
        metaOut.print('\t');
        metaOut.print(meta.md5);
        metaOut.print('\t');
        metaOut.print(meta.url);
        metaOut.print('\t');
        metaOut.print(meta.triples);
        metaOut.print('\t');
        if (meta.parent != null) {
            metaOut.println(meta.parent);
        } else {
            metaOut.println();
        }
    }
}
