package com.entertainment.nifi.processor;

import com.entertainment.nifi.processor.util.XMLSplitByCountUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.List;

/**
 * Created by dwang on 4/17/17.
 */
public class XMLSplitByCountUtilTest {

    XMLSplitByCountUtil xmlSplitByCountUtil;
    String testFile = "target/test-classes/test.xml";
    @Before
    public void setup(){


    }

    @Test
    public void testEqualSplit() {
        InputStream in = null;
        try {
            in = new FileInputStream(new File(testFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        xmlSplitByCountUtil =new XMLSplitByCountUtil(new File("./target/test-classes").toPath(),in, 1, 4, null, null);
        List<File> files = xmlSplitByCountUtil.split();
        assert (files.size()==3);
        for(File f: files) {
            assert Files.exists(f.toPath());
//            try {
//                Files.deleteIfExists(f.toPath());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }

    }

    //not equal split
    @Test
    public void testNoEqualSplit() {
        InputStream in = null;
        try {
            in = new FileInputStream(new File(testFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        xmlSplitByCountUtil =new XMLSplitByCountUtil(new File("./target/test-classes").toPath(),in, 1, 5, null, null);
        List<File> files = xmlSplitByCountUtil.split();
        assert (files.size()==3);
        for(File f: files) {
            assert Files.exists(f.toPath());
//            try {
//                Files.deleteIfExists(f.toPath());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
    }
}
