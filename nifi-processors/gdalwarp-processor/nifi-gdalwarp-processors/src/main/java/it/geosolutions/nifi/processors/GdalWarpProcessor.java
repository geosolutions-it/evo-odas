package it.geosolutions.nifi.processors;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"gdal","gdalwarp","reprojection","retiling"})
@CapabilityDescription("Executes gdalwarp on a file passed via its path through a flow file and creates a new file passing its path in the success relation. It expects to have gdalwarp installed in the system and it works on the local filesystem. More info at http://www.gdal.org/gdalwarp.html")
@DynamicProperty(name = "An environment variable name", value = "An environment variable value", description = "These environment variables are passed to the process spawned by this Processor")
@WritesAttributes({
    @WritesAttribute(attribute = "execution.command", description = "the gdalwarp command executed"),
    @WritesAttribute(attribute = "execution.status", description = "The exit status code returned from executing the command"),
    @WritesAttribute(attribute = "execution.error", description = "Any error messages returned from executing the command")})
@Restricted("Provides operator the ability to execute gdalwarp utility assuming all permissions that NiFi has.")
public class GdalWarpProcessor extends AbstractProcessor {

    public static final Relationship ORIGINAL_RELATIONSHIP = new Relationship.Builder()
            .name("original")
            .description("FlowFiles that were successfully processed")
            .build();
    public static final Relationship OUTPUT_STREAM_RELATIONSHIP = new Relationship.Builder()
            .name("output stream")
            .description("The destination path for the flow file created from the command's output")
            .build();
    private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();

    private final static Set<Relationship> OUTPUT_STREAM_RELATIONSHIP_SET;
    private final static Set<Relationship> ATTRIBUTE_RELATIONSHIP_SET;

    private static final Validator ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR = StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true);
    private static final Validator INTEGER_VALIDATOR = StandardValidators.createLongValidator(64, 4096, true);

    private static final String EXECUTION_COMMAND = "gdalwarp";
    
    /**
     *   -t_srs srs_def:
     */
    static final PropertyDescriptor TARGET_SRS = new PropertyDescriptor.Builder()
            .name("Target SRS")
            .description("The SRS of the output image")
            .expressionLanguageSupported(true)
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                	boolean isValid = true;
                	String[] splitted = input.split(":");
                	if(splitted!=null && splitted.length == 2 && StringUtils.isAlpha(splitted[0]) && StringUtils.isAlphanumeric(splitted[1])){
                		isValid = true;
                	}
                	else{
                		isValid = false;
                	}
                	ValidationResult result = new ValidationResult.Builder()
                    .subject(subject).valid(isValid).input(input).build();
                    return result;
                }
            }).build();
    
	/**
	 *   -co;TILED=YES;
	 *   -co;BLOCKXSIZE=512;
	 *   -co;BLOCKYSIZE=512;
	 */
    static final PropertyDescriptor TILE_SIZE = new PropertyDescriptor.Builder()
            .name("Tile size")
            .description("The size of the internal tiling structure of the output file")
            .expressionLanguageSupported(true)
            .addValidator(INTEGER_VALIDATOR).build();

   /**
    *    inputfile
    */
    static final PropertyDescriptor INPUT_IMG_ABS_PATH = new PropertyDescriptor.Builder()
            .name("The input raster absolute path")
            .description("The input raster to use as input of the gdalwarp command.")
            .expressionLanguageSupported(true)
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                	boolean isValid = (new File(input).exists()) ? true : false;
                	ValidationResult result = new ValidationResult.Builder()
                    .subject(subject).valid(isValid).input(input).build();
                    return result;
                }
            }).build();

    static final PropertyDescriptor OUTPUT_IMG_FILENAME = new PropertyDescriptor.Builder()
            .name("The output raster filename")
            .description("The output raster filename. It will be created inside the workingdir")
            .expressionLanguageSupported(true).build();
    
    static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
            .name("Working Directory")
            .description("The directory to use as the current working directory when executing the command")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, true))
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(TARGET_SRS);
        props.add(TILE_SIZE);
        props.add(INPUT_IMG_ABS_PATH);
        props.add(OUTPUT_IMG_FILENAME);
        props.add(WORKING_DIR);
        PROPERTIES = Collections.unmodifiableList(props);


        Set<Relationship> outputStreamRelationships = new HashSet<>();
        outputStreamRelationships.add(OUTPUT_STREAM_RELATIONSHIP);
        outputStreamRelationships.add(ORIGINAL_RELATIONSHIP);
        OUTPUT_STREAM_RELATIONSHIP_SET = Collections.unmodifiableSet(outputStreamRelationships);

        Set<Relationship> attributeRelationships = new HashSet<>();
        attributeRelationships.add(ORIGINAL_RELATIONSHIP);
        ATTRIBUTE_RELATIONSHIP_SET = Collections.unmodifiableSet(attributeRelationships);
    }

    private ComponentLog logger;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        logger = getLogger();

        relationships.set(OUTPUT_STREAM_RELATIONSHIP_SET);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
        .name(propertyDescriptorName)
        .description("Sets the environment variable '" + propertyDescriptorName + "' for the process' environment")
        .dynamic(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();
        if (null == inputFlowFile) {
            return;
        }

        final String executeCommand = EXECUTION_COMMAND;
        
        StringBuffer sb = new StringBuffer();
        sb.append("-t_srs").append(" ").append(context.getProperty(TARGET_SRS).getValue()).append(" ");
        sb.append("-co TILED=YES").append(" ");
        sb.append("-co BLOCKXSIZE=").append(context.getProperty(TILE_SIZE).getValue()).append(" ");
        sb.append("-co BLOCKYSIZE=").append(context.getProperty(TILE_SIZE).getValue()).append(" ");
        sb.append(context.getProperty(INPUT_IMG_ABS_PATH).getValue()).append(" ");
        sb.append(context.getProperty(WORKING_DIR).getValue()).append(context.getProperty(OUTPUT_IMG_FILENAME).getValue()).append(" ");
        
        final String commandArguments = sb.toString();

        
        final String workingDir = context.getProperty(WORKING_DIR).evaluateAttributeExpressions(inputFlowFile).getValue();

        final ProcessBuilder builder = new ProcessBuilder();

        logger.debug("Executing and waiting for command {} with arguments {}", new Object[]{executeCommand, commandArguments});
        File dir = null;
        if (!isBlank(workingDir)) {
            dir = new File(workingDir);
            if (!dir.exists() && !dir.mkdirs()) {
                logger.warn("Failed to create working directory {}, using current working directory {}", new Object[]{workingDir, System.getProperty("user.dir")});
            }
        }
//        final Map<String, String> environment = new HashMap<>();
//        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
//            if (entry.getKey().isDynamic()) {
//                environment.put(entry.getKey().getName(), entry.getValue());
//            }
//        }
//        builder.environment().putAll(environment);
        builder.command(executeCommand);
        builder.directory(dir);
        builder.redirectInput(Redirect.PIPE);
        builder.redirectOutput(Redirect.PIPE);
        final Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            logger.error("Could not create external process to run command", e);
            throw new ProcessException(e);
        }
        try (final OutputStream pos = process.getOutputStream();
                final InputStream pis = process.getInputStream();
                final InputStream pes = process.getErrorStream();
                final BufferedInputStream bis = new BufferedInputStream(pis);
                final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(pes))) {
            int exitCode = -1;
            final BufferedOutputStream bos = new BufferedOutputStream(pos);
            FlowFile outputFlowFile = inputFlowFile;

            ProcessStreamWriterCallback callback = new ProcessStreamWriterCallback(true, bos, bis, logger,
                    null, session, outputFlowFile, process,false,0);
            session.read(inputFlowFile, callback);

            outputFlowFile = callback.outputFlowFile;
            

            exitCode = callback.exitCode;
            logger.debug("Execution complete for command: {}.  Exited with code: {}", new Object[]{executeCommand, exitCode});

            Map<String, String> attributes = new HashMap<>();

            final StringBuilder strBldr = new StringBuilder();
            try {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    strBldr.append(line).append("\n");
                }
            } catch (IOException e) {
                strBldr.append("Unknown...could not read Process's Std Error");
            }
            int length = strBldr.length() > 4000 ? 4000 : strBldr.length();
            attributes.put("execution.error", strBldr.substring(0, length));

            final Relationship outputFlowFileRelationship = OUTPUT_STREAM_RELATIONSHIP;
            if (exitCode == 0) {
                logger.info("Transferring flow file {} to {}",
                        new Object[]{outputFlowFile,outputFlowFileRelationship.getName()});
            } else {
                logger.error("Transferring flow file {} to {}. Executable command {} ended in an error: {}",
                        new Object[]{outputFlowFile,outputFlowFileRelationship.getName(), executeCommand, strBldr.toString()});
            }

            attributes.put("execution.status", Integer.toString(exitCode));
            attributes.put("execution.command", executeCommand);
            attributes.put("execution.command.args", commandArguments);
            outputFlowFile = session.putAllAttributes(outputFlowFile, attributes);

            // This transfer will transfer the FlowFile that received the stream out put to it's destined relationship.
            // In the event the stream is put to the an attribute of the original, it will be transferred here.
            session.transfer(outputFlowFile, outputFlowFileRelationship);


        } catch (final IOException ex) {
            // could not close Process related streams
            logger.warn("Problem terminating Process {}", new Object[]{process}, ex);
        } finally {
            process.destroy(); // last ditch effort to clean up that process.
        }
    }

    static class ProcessStreamWriterCallback implements InputStreamCallback {

        final boolean ignoreStdin;
        final OutputStream stdinWritable;
        final InputStream stdoutReadable;
        final ComponentLog logger;
        final ProcessSession session;
        final Process process;
        FlowFile outputFlowFile;
        int exitCode;
        final boolean putToAttribute;
        final int attributeSize;
        final String attributeName;

        byte[] outputBuffer;
        int size;

        public ProcessStreamWriterCallback(boolean ignoreStdin, OutputStream stdinWritable, InputStream stdoutReadable,ComponentLog logger, String attributeName,
                                           ProcessSession session, FlowFile outputFlowFile, Process process, boolean putToAttribute, int attributeSize) {
            this.ignoreStdin = ignoreStdin;
            this.stdinWritable = stdinWritable;
            this.stdoutReadable = stdoutReadable;
            this.logger = logger;
            this.session = session;
            this.outputFlowFile = outputFlowFile;
            this.process = process;
            this.putToAttribute = putToAttribute;
            this.attributeSize = attributeSize;
            this.attributeName = attributeName;
        }

        @Override
        public void process(final InputStream incomingFlowFileIS) throws IOException {
            if (putToAttribute) {
                try (SoftLimitBoundedByteArrayOutputStream softLimitBoundedBAOS = new SoftLimitBoundedByteArrayOutputStream(attributeSize)) {
                    readStdoutReadable(ignoreStdin, stdinWritable, logger, incomingFlowFileIS);
                    final long longSize = StreamUtils.copy(stdoutReadable, softLimitBoundedBAOS);

                    // Because the outputstream has a cap that the copy doesn't know about, adjust
                    // the actual size
                    if (longSize > (long) attributeSize) { // Explicit cast for readability
                        size = attributeSize;
                    } else{
                        size = (int) longSize; // Note: safe cast, longSize is limited by attributeSize
                    }

                    outputBuffer = softLimitBoundedBAOS.getBuffer();
                    stdoutReadable.close();

                    try {
                        exitCode = process.waitFor();
                    } catch (InterruptedException e) {
                        logger.warn("Command Execution Process was interrupted", e);
                    }
                }
            } else {
                outputFlowFile = session.write(outputFlowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {

                        readStdoutReadable(ignoreStdin, stdinWritable, logger, incomingFlowFileIS);
                        StreamUtils.copy(stdoutReadable, out);
                        try {
                            exitCode = process.waitFor();
                        } catch (InterruptedException e) {
                            logger.warn("Command Execution Process was interrupted", e);
                        }
                    }
                });
            }
        }
    }

    private static void readStdoutReadable(final boolean ignoreStdin, final OutputStream stdinWritable,
                                           final ComponentLog logger, final InputStream incomingFlowFileIS) throws IOException {
        Thread writerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                if (!ignoreStdin) {
                    try {
                        StreamUtils.copy(incomingFlowFileIS, stdinWritable);
                    } catch (IOException e) {
                        // This is unlikely to occur, and isn't handled at the moment
                        // Bug captured in NIFI-1194
                        logger.error("Failed to write flow file to stdin due to {}", new Object[]{e}, e);
                    }
                }
                // MUST close the output stream to the stdin so that whatever is reading knows
                // there is no more data.
                IOUtils.closeQuietly(stdinWritable);
            }
        });
        writerThread.setDaemon(true);
        writerThread.start();
    }
    
    public static boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
}
