package org.embulk.filter.base58;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Base58FilterPlugin implements FilterPlugin {

    private final Logger logger = Exec.getLogger(Base58FilterPlugin.class);

    public interface PluginTask extends Task {
        @Config("columns")
        List<Base58Column> getColumns();
    }

    public interface Base58Column extends Task {
        @Config("name")
        String getName();

        @Config("encode")
        @ConfigDefault("true")
        Optional<Boolean> getIsEncode();

        @Config("prefix")
        @ConfigDefault("null")
        Optional<String> getPrefix();

        @Config("new_name")
        @ConfigDefault("null")
        Optional<String> getNewName();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = buildOutputSchema(task, inputSchema);
        control.run(task.dump(), outputSchema);
    }

    Schema buildOutputSchema(PluginTask task, Schema inputSchema) {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        // Roll through original columns
        int i = 0;
        for (Column column : inputSchema.getColumns()) {
            builder.add(new Column(i++, column.getName(), column.getType()));
        }

        // Append new columns, if base58 columns desire it
        for (Base58Column column : task.getColumns()) {
            if (column.getNewName().isPresent()) {
                logger.info("added column: name: {}, type: {}, index: {}",
                        column.getNewName().get(),
                        Types.STRING,
                        i);
                builder.add(new Column(i++, column.getNewName().get(), Types.STRING));
            } else {
                logger.info("overriding column: name: {}", column.getName());
            }
        }

        return new Schema(builder.build());
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output) {

        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Map<String, List<Base58Column>> base58ColumnMap = convertBase58ColumnListToMap(task.getColumns());
        final Map<String, Column> outputColumnMap = convertColumnListToMap(outputSchema.getColumns(), logger);

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    setValue(base58ColumnMap, outputColumnMap, reader, outputSchema, builder);
                    builder.addRecord();
                }
            }

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }
        };
    }

    void setValue(final Map<String, List<Base58Column>> base58ColumnMap, final Map<String, Column> outputColumnMap, final PageReader reader, final Schema outputSchema, final PageBuilder builder) {

        // Build a map of output columns that should be mapped to encoded/decoded original columns
        final Map<String, Base58Column> modifiedColumnMap = new HashMap<>(); // output column -> base58 column config
        for (List<Base58Column> base58SourceColumns : base58ColumnMap.values()) {
            for (Base58Column base58Column : base58SourceColumns) {
                String outputColumnName = base58Column.getNewName().isPresent() ? base58Column.getNewName().get() : base58Column.getName();
                modifiedColumnMap.put(outputColumnName, base58Column);
            }
        }

        // Set values on all our output columns
        List<Column> columns = outputSchema.getColumns();
        for (Column outputColumn : columns) {

            // Convert the value if configured to do so
            if (modifiedColumnMap.containsKey(outputColumn.getName())) {
                Base58Column base58Column = modifiedColumnMap.get(outputColumn.getName());
                Column sourceColumn = outputColumnMap.get(base58Column.getName());
                String inputValue, convertedValue = null;

                // Don't bother setting it if the source is null
                if (reader.isNull(sourceColumn)) {
                    builder.setNull(outputColumn);
                    continue;
                }

                // Get the source value
                if (Types.STRING.equals(sourceColumn.getType())) {
                    inputValue = reader.getString(sourceColumn);
                } else {
                    logger.error("cannot convert base58 value of non-string values. name: {}, type: {}, index: {}",
                            sourceColumn.getName(),
                            sourceColumn.getType(),
                            sourceColumn.getIndex());
                    throw new DataException("Unexpected non-string type in column `" + sourceColumn.getName() + "`. Got: " + sourceColumn.getType());
                }

                // Convert the source value
                try {
                    convertedValue = convertValue(inputValue, base58Column.getIsEncode().or(true), base58Column.getPrefix().or(""));
                } catch (Exception e) {
                    // Failed to do the conversion. Probably misconfigured or malformed value
                    logger.error("failed to encode/decode base58 column value. name: {}, type: {}, index: {}, value: {}, method: {}, prefix: {}, target_name: {}",
                            sourceColumn.getName(),
                            sourceColumn.getType(),
                            sourceColumn.getIndex(),
                            inputValue,
                            base58Column.getIsEncode().get() ? "encode" : "decode",
                            base58Column.getPrefix(),
                            base58Column.getNewName().or(base58Column.getName()));
                    logger.error("base58 conversion exception", e);
                    // Don't crash the import if a single value is screwed up. Just log it for now
                }

                if (convertedValue == null) {
                    builder.setNull(outputColumn);
                    continue;
                } else {
                    builder.setString(outputColumn, convertedValue);
                    continue;
                }
            }

            // No value?
            if (reader.isNull(outputColumn)) {
                builder.setNull(outputColumn);
                continue;
            }

            // Inherit value
            if (Types.STRING.equals(outputColumn.getType())) {
                builder.setString(outputColumn, reader.getString(outputColumn));
            }
            else if (Types.BOOLEAN.equals(outputColumn.getType())) {
                builder.setBoolean(outputColumn, reader.getBoolean(outputColumn));
            }
            else if (Types.DOUBLE.equals(outputColumn.getType())) {
                builder.setDouble(outputColumn, reader.getDouble(outputColumn));
            }
            else if (Types.LONG.equals(outputColumn.getType())) {
                builder.setLong(outputColumn, reader.getLong(outputColumn));
            }
            else if (Types.TIMESTAMP.equals(outputColumn.getType())) {
                builder.setTimestamp(outputColumn, reader.getTimestamp(outputColumn));
            }
            else if (Types.JSON.equals(outputColumn.getType())) {
                builder.setJson(outputColumn, reader.getJson(outputColumn));
            } else {
                // NO VALUE? It shall be null.
                builder.setNull(outputColumn);
            }
        }
    }

    String convertValue(String value, Boolean isEncode, String prefix) {
        if (isEncode) {
            return Base58.encodeWithPrefix(value, prefix);
        } else {
            return Base58.decodeWithPrefix(value, prefix);
        }
    }

    static Map<String, List<Base58Column>> convertBase58ColumnListToMap(List<Base58Column> base58Columns) {
        Map<String, List<Base58Column>> result = new HashMap<>();
        for (Base58Column base58Column : base58Columns) {
            if (!result.containsKey(base58Column.getName())) {
                result.put(base58Column.getName(), new ArrayList<>());
            }
            result.get(base58Column.getName()).add(base58Column);
        }
        return result;
    }

    static Map<String, Column> convertColumnListToMap(List<Column> columns, Logger logger) {
        Map<String, Column> result = new HashMap<>();
        for (Column column : columns) {
            if (result.containsKey(column.getName())) {
                logger.warn("Output column ({}) already defined. Do you have duplicate column names in your source?", column.getName());
            }
            result.put(column.getName(), column);
        }
        return result;
    }
}
