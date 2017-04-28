package com.entertainment.nifi.processor;

import com.entertainment.nifi.processor.util.XMLSplitByCountUtil;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

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
    @Test
    public void testOnTrigger() throws IOException {
        TestRunner runner= TestRunners.newTestRunner(new XMLSplitter());

        runner.setValidateExpressionUsage(false);
        runner.setProperty(XMLSplitter.SPLIT_DEPTH, "1");
        runner.setProperty(XMLSplitter.SPLIT_COUNT, "4");
        Path inputXML = FileSystems.getDefault().getPath("target/test-classes/test.xml");
        runner.enqueue(inputXML);
        runner.run(1);

        runner.assertQueueEmpty();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(XMLSplitter.REL_SPLIT);
        System.out.println("Number of splitted files: "+results.size());
        for(MockFlowFile file: results) {
            String filename = file.getAttribute("filename");
            assert filename.startsWith(XMLSplitByCountUtil.PREFIX);
        }
        assert results.size()>0;
    }
}
