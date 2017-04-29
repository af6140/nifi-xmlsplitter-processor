package com.entertainment.nifi.processor;

import com.entertainment.nifi.processor.util.XMLSplitByCountUtil;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.nifi.flowfile.attributes.CoreAttributes;



/**
 * Created by dwang on 4/17/17.
 */
@Tags({"XML", "Split"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Split large xml files into chunks by element count")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count",
                description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
public class XMLSplitter extends AbstractProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final String FRAGMENT_IDENTIFIER = "fragment.identifier";
    public static final String FRAGMENT_INDEX = "fragment.index";
    public static final String FRAGMENT_COUNT = "fragment.count";
    public static final String SEGMENT_ORIGINAL_FILENAME="segment.original.filename";

    public static final PropertyDescriptor SPLIT_DEPTH = new PropertyDescriptor.Builder()
            .name("Split Depth")
            .description("Indicates the XML-nesting depth to start splitting XML fragments. A depth of 1 means split the root's children, whereas a depth of"
                    + " 2 means split the root's children's children and so forth.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor SPLIT_COUNT = new PropertyDescriptor.Builder()
            .name("Split Count")
            .description("How many elements at depth in one split file.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
            .name("Header")
            .description("Header to prepend, usually parent xml opening tags.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();
    public static final PropertyDescriptor FOOTER = new PropertyDescriptor.Builder()
            .name("Footer")
            .description("Footer to append, usually parent xml closing tags.")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor WORK_DIR = new PropertyDescriptor.Builder()
            .name("Work Dir")
            .description("Footer to append, usually parent xml closing tags.")
            .addValidator(StandardValidators.DirectoryExistsValidator.VALID)
            .required(false)
            .expressionLanguageSupported(true)
            .build();


    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All segments of the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship")
            .build();
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SPLIT_DEPTH);
        properties.add(SPLIT_COUNT);
        properties.add(HEADER);
        properties.add(FOOTER);
        properties.add(WORK_DIR);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SPLIT);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final int depth = context.getProperty(SPLIT_DEPTH).evaluateAttributeExpressions(original).asInteger();
        final int count = context.getProperty(SPLIT_COUNT).evaluateAttributeExpressions(original).asInteger();

        final String header = context.getProperty(HEADER).evaluateAttributeExpressions(original).getValue();
        final String footer = context.getProperty(FOOTER).evaluateAttributeExpressions(original).getValue();
        final String workDir = context.getProperty(WORK_DIR).evaluateAttributeExpressions(original).getValue();
        final ComponentLog logger = getLogger();

        final List<FlowFile> splits = new ArrayList<>();
        final String fragmentIdentifier = UUID.randomUUID().toString();
        final AtomicInteger numberOfRecords = new AtomicInteger(0);
        final AtomicBoolean failed = new AtomicBoolean(false);
        List<Path> splitted=new LinkedList<Path>();
        session.read(original, rawIn -> {
            try (final InputStream in = new BufferedInputStream(rawIn)) {
                String realWorkDir = workDir==null ? System.getProperty("java.io.tmpdir") : workDir;
                final XMLSplitByCountUtil splitter = new XMLSplitByCountUtil(FileSystems.getDefault().getPath(realWorkDir), in, depth, count, header,footer);
                final List<File> results = splitter.split();
                for(File f: results) {
                    splitted.add(f.toPath());
                }
            }
        });
        for(Path splitFile: splitted) {
            FlowFile n_split = session.create(original);
            FlowFile split = session.importFrom(splitFile, false, n_split);
            split = session.putAttribute(split, FRAGMENT_IDENTIFIER, fragmentIdentifier);
            split = session.putAttribute(split, FRAGMENT_INDEX, Integer.toString(numberOfRecords.getAndIncrement()));
            split = session.putAttribute(split, SEGMENT_ORIGINAL_FILENAME, split.getAttribute(CoreAttributes.FILENAME.key()));
            splits.add(split);
        }
        if (failed.get()) {
            session.transfer(original, REL_FAILURE);
            session.remove(splits);
        } else {
            splits.forEach((split) -> {
                split = session.putAttribute(split, FRAGMENT_COUNT, Integer.toString(numberOfRecords.get()));
                session.transfer(split, REL_SPLIT);
            });

            //final FlowFile originalToTransfer = copyAttributesToOriginal(session, original, fragmentIdentifier, numberOfRecords.get());
            session.transfer(original, REL_ORIGINAL);
            logger.info("Split {} into {} FlowFiles", new Object[]{original, splits.size()});
        }

    }
}
