package com.tresata.cascading.opencsv;

import java.io.StringWriter;
import java.io.IOException;
import java.nio.charset.Charset;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class OpenCsvScheme extends TextLine {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCsvScheme.class);

    private final boolean hasHeader;
    private final char separator;
    private final char quote;
    private final char escape;
    private final boolean strict;
    private final String charsetName;

    public OpenCsvScheme(final Fields fields, final boolean hasHeader, final char separator, final char quote, final char escape, 
                         final boolean strict, final String charsetName) {
        super(TextLine.Compress.DEFAULT);
        setCharsetName(charsetName);
        setSinkFields(fields);
        setSourceFields(fields);
        
        this.hasHeader = hasHeader;
        this.separator = separator;
        this.quote = quote;
        this.escape = escape;
        this.strict = strict;
        this.charsetName = charsetName != null ? charsetName : "UTF8";
    }
    
    /**
     * Use for files without headers.
     */
    public OpenCsvScheme(final Fields fields, final char separator, final char quote, final char escape, final boolean strict) {
        this(fields, false, separator, quote, escape, strict, null);
    }

    /**
     * Use for comma separated, quoted and slash (\) escaped files without headers.
     */
    public OpenCsvScheme(final Fields fields) {
        this(fields, false, ',', '"', '\\', false, null);
    }

    /**
     * Use for files with headers.
     */
    public OpenCsvScheme(final char separator, final char quote, final char escape, final boolean strict) {
        this(Fields.UNKNOWN, true, separator, quote, escape, strict, null);
    }

    /**
     * Use for comma separated, quoted and slash (\) escaped files with headers.
     */
    public OpenCsvScheme() {
        this(Fields.UNKNOWN, true, ',', '"', '\\', false, null);
    }
    
    public boolean hasHeader() {
        return hasHeader;
    }

    public char getSeparator() {
        return separator;
    }

    public char getQuote() {
        return quote;
    }

    public char getEscape() {
        return escape;
    }

    private CSVParser createCsvParser() {
        return new CSVParser(separator, quote, escape);
    }

    @SuppressWarnings("unchecked")
    private Fields parseFirstLine(final FlowProcess<JobConf> flowProcess, final Tap tap) {
        // no need to open them all
        final Tap singleTap = tap instanceof CompositeTap ? (Tap) ((CompositeTap) tap).getChildTaps().next() : tap;
        
        // should revert to file:// (Lfs) if tap is Lfs
        final Tap textLineTap = new Hfs(new TextLine(new Fields("line"), charsetName), tap.getFullIdentifier(flowProcess.getConfigCopy()));
        
        try {
            final TupleEntryIterator iterator = textLineTap.openForRead(flowProcess);
            try {
                final String[] result = createCsvParser().parseLine(iterator.next().getTuple().getString(0));
                return new Fields(result);
            } catch (IOException exc) {
                throw new RuntimeException(exc);
            } finally {
                iterator.close();
            }
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
    
    @Override
    public Fields retrieveSourceFields(final FlowProcess<JobConf> flowProcess, final Tap tap) {
        if (hasHeader && getSourceFields().isUnknown())
            setSourceFields(parseFirstLine(flowProcess, tap));
        return getSourceFields();
    }
    
    @Override
    public void sourcePrepare(final FlowProcess<JobConf> flowProcess, final SourceCall<Object[], RecordReader> sourceCall) {
        sourceCall.setContext(new Object[4]);
        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
        sourceCall.getContext()[2] = Charset.forName(charsetName);
        sourceCall.getContext()[3] = createCsvParser();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean source(final FlowProcess<JobConf> flowProcess, final SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        final Object[] context = sourceCall.getContext();

        while (sourceCall.getInput().next(context[0], context[1])) {
            if (hasHeader && ((LongWritable) context[0]).get() == 0)
                continue;
            final String[] split;
            try {
                split = ((CSVParser) sourceCall.getContext()[3]).parseLine(makeEncodedString(context));
            } catch (IOException exc) {
                if (strict)
                    throw exc;
                LOGGER.warn("exception", exc);
                flowProcess.increment("com.tresata.cascading.scheme.OpenCsvScheme", "Invalid Records", 1);
                continue;
            }
            for (int i = 0; i < split.length; i++)
                if (split[i].equals(""))
                    split[i] = null;
            final Tuple tuple = sourceCall.getIncomingEntry().getTuple();
            tuple.clear();
            tuple.addAll(split);
            if (tuple.size() != getSourceFields().size()) {
                if (strict)
                    throw new TapException(String.format("expected %s items but got %s", getSourceFields().size(), tuple.size()), tuple);
                LOGGER.warn("expected {} items but got {}, tuple: {}", new Object[]{getSourceFields().size(), tuple.size() , tuple});
                flowProcess.increment("com.tresata.cascading.scheme.OpenCsvScheme", "Invalid Records", 1);
                tuple.clear(); // probably not necessary
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public void presentSinkFields(final FlowProcess<JobConf> flowProcess, final Tap tap, final Fields fields) {
        presentSinkFieldsInternal(fields);
    }
    
    @Override
    public void sinkPrepare(final FlowProcess<JobConf> flowProcess, final SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        final StringWriter stringWriter = new StringWriter(4 * 1024);
        final CSVWriter csvWriter = new CSVWriter(stringWriter, separator, quote, escape);
        sinkCall.setContext(new Object[5]);
        sinkCall.getContext()[0] = new Text();
        sinkCall.getContext()[1] = stringWriter;
        sinkCall.getContext()[2] = Charset.forName(charsetName);
        sinkCall.getContext()[3] = csvWriter;
        sinkCall.getContext()[4] = new String[getSinkFields().size()];

        if (hasHeader) {
            final Fields fields = sinkCall.getOutgoingEntry().getFields();
            write(sinkCall, fields);
        }
    }
    
    @Override
    public void sink(final FlowProcess<JobConf> flowProcess, final SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();
        write(sinkCall, tuple);
    }
        
    @SuppressWarnings("unchecked")
    private void write(final SinkCall<Object[], OutputCollector> sinkCall, final Iterable<? extends Object> value) throws IOException {
        final Text text = (Text) sinkCall.getContext()[0];
        final StringWriter stringWriter = (StringWriter) sinkCall.getContext()[1];
        final Charset charset = (Charset) sinkCall.getContext()[2];
        final CSVWriter csvWriter = (CSVWriter) sinkCall.getContext()[3];
        final String[] nextLine = (String[]) sinkCall.getContext()[4];
        stringWriter.getBuffer().setLength(0);
        int i = 0;
        for (Object item: value) {
            nextLine[i] = item == null ? "" : item.toString();
            i++;
        }
        csvWriter.writeNext(nextLine);
        final int l = stringWriter.getBuffer().length();
        stringWriter.getBuffer().setLength(l > 0 ? l - 1 : 0);
        text.set(stringWriter.getBuffer().toString().getBytes(charset));
        sinkCall.getOutput().collect(null, text);
    }
}
