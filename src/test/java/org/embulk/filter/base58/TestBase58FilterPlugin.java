package org.embulk.filter.base58;

import com.google.common.collect.Lists;

import org.embulk.EmbulkEmbed;
import org.embulk.EmbulkTestRuntime;
//import org.embulk.config.ConfigException;
import org.embulk.filter.base58.Base58FilterPlugin.PluginTask;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.spi.type.Types.BOOLEAN;
//import static org.embulk.spi.type.Types.DOUBLE;
//import static org.embulk.spi.type.Types.JSON;
//import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
//import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class TestBase58FilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Base58FilterPlugin plugin;

    @Before
    public void createResource()
    {
        plugin = new Base58FilterPlugin();
    }

    private Schema schema(Column... columns)
    {
        return new Schema(Lists.newArrayList(columns));
    }

    private ConfigSource configFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yamlString);
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        ConfigSource config = configFromYamlString(lines);
        return config.loadConfig(PluginTask.class);
    }

    @Test
    public void buildOutputSchema_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .build();

        Schema outputSchema = plugin.buildOutputSchema(task, inputSchema);
        assertEquals(1, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("_id", column.getName());
        }
    }

    @Test
    public void buildOutputSchemaDecode_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: public_id, encode: false}");
        Schema inputSchema = Schema.builder()
                .add("public_id", STRING)
                .build();

        Schema outputSchema = plugin.buildOutputSchema(task, inputSchema);
        assertEquals(1, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("public_id", column.getName());
        }
    }

    @Test
    public void buildOutputSchemaPrefix_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id, prefix: obj_}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .build();

        Schema outputSchema = plugin.buildOutputSchema(task, inputSchema);
        assertEquals(1, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("_id", column.getName());
        }
    }

    @Test
    public void buildOutputSchemaDecodePrefix_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: public_id, encode: false, prefix: obj_}");
        Schema inputSchema = Schema.builder()
                .add("public_id", STRING)
                .build();

        Schema outputSchema = plugin.buildOutputSchema(task, inputSchema);
        assertEquals(1, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("public_id", column.getName());
        }
    }

    @Test
    public void buildOutputSchemaPrefixNewColumn_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: _id, prefix: obj_, new_name: public_id}");
        Schema inputSchema = Schema.builder()
                .add("_id", STRING)
                .build();

        Schema outputSchema = plugin.buildOutputSchema(task, inputSchema);
        assertEquals(2, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("_id", column.getName());
            column = outputSchema.getColumn(1);
            assertEquals("public_id", column.getName());
        }
    }

    @Test
    public void buildOutputSchemaDecodePrefixNewColumn_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: base58",
                "columns:",
                "  - {name: public_id, encode: false, prefix: obj_, new_name: _id}");
        Schema inputSchema = Schema.builder()
                .add("public_id", STRING)
                .build();

        Schema outputSchema = plugin.buildOutputSchema(task, inputSchema);
        assertEquals(2, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("public_id", column.getName());
            column = outputSchema.getColumn(1);
            assertEquals("_id", column.getName());
        }
    }
}
