package org.apache.flume.test.agent;

import org.apache.commons.io.FileUtils;
import org.apache.flume.test.util.StagedInstall;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestInterceptors {
    private Properties agentProps;
    private File sinkOutputDir;

    private static final Logger LOGGER = LoggerFactory.getLogger(TestInterceptors.class);

    @Before
    public void setup() throws Exception {

        File agentDir = StagedInstall.getInstance().getStageDir();
        LOGGER.debug("Using agent stage dir: {}", agentDir);

        File testDir = new File(agentDir, TestInterceptors.class.getName());
        if (testDir.exists()) {
            FileUtils.deleteDirectory(testDir);
        }
        assertTrue(testDir.mkdirs());

        agentProps = new Properties();

        // Create the rest of the properties file
        agentProps.put("agent.sources.seq-01.type", "seq");
        agentProps.put("agent.sources.seq-01.totalEvents", "100");
        agentProps.put("agent.sources.seq-01.channels", "mem-01");
        agentProps.put("agent.sources.seq-01.interceptors", "ski-01");
        agentProps.put("agent.sources.seq-01.interceptors.ski-01.type", "static");
        agentProps.put("agent.sources.seq-01.interceptors.ski-01.key", "interceptor1");
        agentProps.put("agent.sources.seq-01.interceptors.ski-01.value", "source");
        agentProps.put("agent.channels.mem-01.type", "MEMORY");
        agentProps.put("agent.channels.mem-01.capacity", String.valueOf(100000));

        sinkOutputDir = new File(testDir, "out");
        assertTrue("Unable to create sink output dir: " + sinkOutputDir.getPath(),
                sinkOutputDir.mkdir());

        agentProps.put("agent.sinks.roll-01.channel", "mem-01");
        agentProps.put("agent.sinks.roll-01.interceptors", "ski-01");
        agentProps.put("agent.sinks.roll-01.interceptors.ski-01.type", "static");
        agentProps.put("agent.sinks.roll-01.interceptors.ski-01.key", "interceptor2");
        agentProps.put("agent.sinks.roll-01.interceptors.ski-01.value", "sink");
        agentProps.put("agent.sinks.roll-01.type", "FILE_ROLL");
        agentProps.put("agent.sinks.roll-01.sink.directory", sinkOutputDir.getAbsolutePath());
        agentProps.put("agent.sinks.roll-01.sink.rollInterval", "0");
        agentProps.put("agent.sinks.roll-01.sink.serializer", "HEADER_AND_TEXT");


        agentProps.put("agent.sources", "seq-01");
        agentProps.put("agent.channels", "mem-01");
        agentProps.put("agent.sinks", "roll-01");
    }

    @After
    public void teardown() throws Exception {
        StagedInstall.getInstance().stopAgent();
    }

    private void validateSeenEvents(File outDir, int outFiles, int events)
            throws IOException {
        File[] sinkOutputDirChildren = outDir.listFiles();
        assertEquals("Unexpected number of files in output dir",
                outFiles, sinkOutputDirChildren.length);
        Set<String> seenEvents = new HashSet<>();
        for (File outFile : sinkOutputDirChildren) {
            Scanner scanner = new Scanner(outFile);
            while (scanner.hasNext()) {
                seenEvents.add(scanner.nextLine());
            }
        }
        for (int event = 0; event < events; event++) {
            assertTrue(
                    "Missing event: {" + event + "}",
                    seenEvents.contains(String.valueOf("{interceptor2=sink, interceptor1=source} " + event))
            );
        }
    }


    @Test
    public void testInterceptors() throws Exception {
        LOGGER.debug("testInterceptors() started.");

        StagedInstall.getInstance().startAgent("agent", agentProps);

        TimeUnit.SECONDS.sleep(10); // Wait for sources and sink to process files

        // Ensure we received all events.
        validateSeenEvents(sinkOutputDir, 1, 100);
        LOGGER.debug("Processed all the events!");

        LOGGER.debug("testConfigReplacement() ended.");
    }
}
