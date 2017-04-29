package com.entertainment.nifi.processor;

import com.entertainment.nifi.processor.util.XMLSplitByCountUtil;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;

/**
 * Created by dwang on 4/21/17.
 */
public class XMLSplitterTest {
    private static Logger logger= LoggerFactory.getLogger(XMLSplitter.class);
    @Test
    public void testOnTrigger() throws IOException {
        TestRunner runner= TestRunners.newTestRunner(new XMLSplitter());

        runner.setValidateExpressionUsage(false);
        runner.setProperty(XMLSplitter.SPLIT_DEPTH, "1");
        runner.setProperty(XMLSplitter.SPLIT_COUNT, "4");
        runner.setProperty(XMLSplitter.HEADER, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<catalog>");
        runner.setProperty(XMLSplitter.FOOTER, "</catalog>");
        Path inputXML = FileSystems.getDefault().getPath("target/test-classes/test.xml");
        runner.enqueue(inputXML);
        runner.run(1);

        runner.assertQueueEmpty();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(XMLSplitter.REL_SPLIT);
        logger.info("Number of splitted files: "+results.size());
        for(MockFlowFile file: results) {
            String filename = file.getAttribute("filename");
            assert filename.startsWith(XMLSplitByCountUtil.PREFIX);
        }
        assert results.size()>0;
    }
}
