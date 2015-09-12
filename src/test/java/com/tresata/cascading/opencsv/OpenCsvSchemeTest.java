package com.tresata.cascading.opencsv;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.pipe.Each;
import cascading.operation.Debug;
import cascading.tap.hadoop.Hfs;
import cascading.tap.Tap;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.Tuple;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class OpenCsvSchemeTest {
    static final String DATA_DIR = "test/data";
    static final String COMPARE_DIR = "test/compare";
    static final String TMP_DIR = "tmp";

    public static boolean compareTaps(final Tap source1, final Tap source2, final Configuration conf) throws IOException {
        final FlowProcess flowProcess1 = new HadoopFlowProcess(new JobConf(conf));
        source1.getScheme().retrieveSourceFields(flowProcess1, source1);
        final TupleEntryIterator iter1 = source1.openForRead(new HadoopFlowProcess(new JobConf(conf)));
        final FlowProcess flowProcess2 = new HadoopFlowProcess(new JobConf(conf));
        source2.getScheme().retrieveSourceFields(flowProcess2, source2);
        final TupleEntryIterator iter2 = source2.openForRead(new HadoopFlowProcess(new JobConf(conf)));
        if (!iter1.getFields().equals(iter2.getFields()))
            return false;
        List<Tuple> list1 = new ArrayList<Tuple>();
        while (iter1.hasNext())
            list1.add(new Tuple(iter1.next().getTuple()));
        iter1.close();
        Collections.sort(list1);
        List<Tuple> list2 = new ArrayList<Tuple>();
        while (iter2.hasNext())
            list2.add(new Tuple(iter2.next().getTuple()));
        iter2.close();
        Collections.sort(list2);
        return list1.equals(list2);
    }
    
    @Test
    public void testWithHeader() throws Exception {
        final String inputFile = "quoted_header.csv";
        final String outputDir = "quoted_header";
        final String compareFile = "quoted_header.csv";
        final Properties props = new Properties();
        final Configuration conf = new Configuration();

        final Tap source = new Hfs(new OpenCsvScheme(), DATA_DIR + "/" + inputFile, SinkMode.KEEP);
        final Tap sink = new Hfs(new OpenCsvScheme(), TMP_DIR + "/" + outputDir, SinkMode.REPLACE);
        final Pipe pipe = new Each(new Pipe("test"), new Debug());
        new HadoopFlowConnector(props).connect(source, sink, pipe).complete();
        
        final Tap compare = new Hfs(new OpenCsvScheme(), COMPARE_DIR + "/" + compareFile, SinkMode.KEEP);
        assertTrue(compareTaps(sink, compare, conf) == true);
    }

    @Test
    public void testHeaderless() throws Exception {
        final String inputFile = "quoted_headerless.csv";
        final String outputDir = "quoted_headerless";
        final String compareFile = "quoted_headerless.csv";
        final Properties props = new Properties();
        final Configuration conf = new Configuration();

        final Tap source = new Hfs(new OpenCsvScheme(new Fields("id", "product", "descr")), DATA_DIR + "/" + inputFile, SinkMode.KEEP);
        final Tap sink = new Hfs(new OpenCsvScheme(new Fields("id", "product", "descr")), TMP_DIR + "/" + outputDir, SinkMode.REPLACE);
        final Pipe pipe = new Each(new Pipe("test"), new Debug());
        new HadoopFlowConnector(props).connect(source, sink, pipe).complete();

        final Tap compare = new Hfs(new OpenCsvScheme(new Fields("id", "product", "descr")), COMPARE_DIR + "/" + compareFile, SinkMode.KEEP);
        assertTrue(compareTaps(sink, compare, conf) == true);
    }

}
