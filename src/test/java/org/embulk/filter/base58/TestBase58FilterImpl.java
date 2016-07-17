package org.embulk.filter.base58;

import com.google.common.collect.Lists;

import org.embulk.EmbulkEmbed;
import org.embulk.EmbulkTestRuntime;
//import org.embulk.config.ConfigException;
import org.embulk.filter.base58.Base58FilterPlugin.PluginTask;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.ValueFactory;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

public class TestBase58FilterImpl {

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Base58FilterPlugin plugin;

    @Before
    public void createResource()
    {
        plugin = new Base58FilterPlugin();
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return config.loadConfig(PluginTask.class);
    }

    private List<Object[]> filter(PluginTask task, Schema inputSchema, Object ... objects)
    {
        MockPageOutput output = new MockPageOutput();
        Schema outputSchema = plugin.buildOutputSchema(task, inputSchema);
        PageBuilder pageBuilder = new PageBuilder(runtime.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        final Map<String, Base58FilterPlugin.Base58Column> base58ColumnMap = Base58FilterPlugin.convertBase58ColumnListToMap(task.getColumns());
        final Map<String, Column> outputColumnMap = Base58FilterPlugin.convertColumnListToMap(outputSchema.getColumns());

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, objects);
        for (Page page : pages) {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                plugin.setValue(base58ColumnMap, outputColumnMap, pageReader, outputSchema, pageBuilder);
                pageBuilder.addRecord();
            }
        }
        pageBuilder.finish();
        pageBuilder.close();
        return Pages.toObjects(outputSchema, output.pages);
    }

    @Test
    public void basicEncoding()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
//                Timestamp.ofEpochSecond(0), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"), "remove_me",
                "54f5f8b37c158c2f12ee1c64");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
//            assertEquals(Timestamp.ofEpochSecond(0), record[0]);
            assertEquals("2bzSwY8SCsogbNxZZ", record[0]);
//            assertEquals(new Boolean(true), record[2]);
//            assertEquals(new Long(0), record[3]);
//            assertEquals(new Double(0.5), record[4]);
//            assertEquals(ValueFactory.newString("json"), record[5]);
        }
    }

    @Test
    public void basicDecoding()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: public_id, encode: false}");
        Schema inputSchema = Schema.builder()
                .add("public_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "2bzSwY8SCsogbNxZZ");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
            assertEquals("54f5f8b37c158c2f12ee1c64", record[0]);
        }
    }



    @Test
    public void playsNiceWithOtherTypes()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .add("created", TIMESTAMP)
                .add("updated", TIMESTAMP)
                .add("is_dead", BOOLEAN)
                .add("count", LONG)
                .add("price", DOUBLE)
                .add("meta", JSON)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "54f5f8b37c158c2f12ee1c64", Timestamp.ofEpochSecond(0), null, new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"));

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(7, record.length);
            assertEquals("2bzSwY8SCsogbNxZZ", record[0]);
            assertEquals(Timestamp.ofEpochSecond(0), record[1]);
            assertEquals(null, record[2]);
            assertEquals(new Boolean(true), record[3]);
            assertEquals(new Long(0), record[4]);
            assertEquals(new Double(0.5), record[5]);
            assertEquals(ValueFactory.newString("json"), record[6]);
        }
    }

    @Test(expected = org.embulk.spi.DataException.class)
    public void doesNotWorkOnNonStringColumn()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: not_id}");
        Schema inputSchema = Schema.builder()
                .add("not_id", BOOLEAN)
                .build();

        filter(task, inputSchema,
                new Boolean(true));
    }

    @Test
    public void basicEncodingWithPrefix()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id, prefix: obj_}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "54f5f8b37c158c2f12ee1c64");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
            assertEquals("obj_2bzSwY8SCsogbNxZZ", record[0]);
        }
    }

    @Test
    public void basicDecodingWithPrefix()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: public_id, encode: false, prefix: obj_}");
        Schema inputSchema = Schema.builder()
                .add("public_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "obj_2bzSwY8SCsogbNxZZ");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
            assertEquals("54f5f8b37c158c2f12ee1c64", record[0]);
        }
    }

    @Test
    public void basicEncodingWithPrefixAndNewName()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id, prefix: obj_, new_name: public_id}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "00f5f8b37c158c2f12ee1c64");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(2, record.length);
            assertEquals("00f5f8b37c158c2f12ee1c64", record[0]);
            assertEquals("obj_123zhNEUWPr5ogRQP", record[1]);
        }
    }

    @Test
    public void basicDecodingWithPrefixAndNewName()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: public_id, encode: false, prefix: obj_, new_name: _id}");
        Schema inputSchema = Schema.builder()
                .add("public_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "obj_123zhNEUWPr5ogRQP");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(2, record.length);
            assertEquals("obj_123zhNEUWPr5ogRQP", record[0]);
            assertEquals("00f5f8b37c158c2f12ee1c64", record[1]);
        }
    }

    @Test
    public void badBase58DecodeTurnsColumnValueNull()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: public_id, encode: false}");
        Schema inputSchema = Schema.builder()
                .add("public_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "I");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
            assertEquals(null, record[0]);
        }
    }

    @Test
    public void badHexValueTurnsNull()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id, encode: true}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .build();

        List<Object[]> records = filter(task, inputSchema,
                "nope");

        assertEquals(1, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(1, record.length);
            assertEquals(null, record[0]);
        }
    }

    @Test
    public void base58EdgeCases()
    {
        assertEquals("00", Base58.decode(""));
        assertEquals(null, Base58.decode("I"));
        assertEquals("1", Base58.encode("00"));
        assertEquals("2", Base58.encode("01"));
    }

}
