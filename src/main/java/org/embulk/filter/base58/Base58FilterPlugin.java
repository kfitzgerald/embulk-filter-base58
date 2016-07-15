package org.embulk.filter.base58;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
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
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Base58FilterPlugin implements FilterPlugin {

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
        @ConfigDefault("")
        Optional<String> getPrefix();

        @Config("new_name")
        @ConfigDefault("null")
        Optional<String> getNewName();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {

        PluginTask task = config.loadConfig(PluginTask.class);
        Map<String, Base58Column> base58ColumnMap = convertBase58ColumnListToMap(task.getColumns());

        Schema.Builder builder = Schema.builder();
        for (Column column : inputSchema.getColumns()) {

            Base58Column base58Column = base58ColumnMap.get(column.getName());

            if (base58Column != null) {
                builder.add(base58Column.getNewName().or(column.getName()), Types.STRING);
            } else {
                builder.add(column.getName(), column.getType());
            }
        }
        control.run(task.dump(), builder.build());
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output) {

        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Map<String, Base58Column> base58ColumnMap = convertBase58ColumnListToMap(task.getColumns());
        final Map<String, Column> outputColumnMap = convertColumnListToMap(outputSchema.getColumns());

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    setValue();
                    builder.addRecord();
                }
            }

            private void setValue() {
                for (Column inputColumn : inputSchema.getColumns()) {
                    if (reader.isNull(inputColumn)) {
                        builder.setNull(inputColumn);
                        continue;
                    }

                    // Write the original data
                    Object inputValue;
                    if (Types.STRING.equals(inputColumn.getType())) {
                        final String value = reader.getString(inputColumn);
                        inputValue = value;
                        builder.setString(inputColumn, value);
                    } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                        final boolean value = reader.getBoolean(inputColumn);
                        inputValue = value;
                        builder.setBoolean(inputColumn, value);
                    } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                        final double value = reader.getDouble(inputColumn);
                        inputValue = value;
                        builder.setDouble(inputColumn, value);
                    } else if (Types.LONG.equals(inputColumn.getType())) {
                        final long value = reader.getLong(inputColumn);
                        inputValue = value;
                        builder.setLong(inputColumn, value);
                    } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                        final Timestamp value = reader.getTimestamp(inputColumn);
                        inputValue = value;
                        builder.setTimestamp(inputColumn, value);
                    } else if (Types.JSON.equals(inputColumn.getType())) {
                        final Value value = reader.getJson(inputColumn);
                        inputValue = value;
                        builder.setJson(inputColumn, value);
                    } else {
                        throw new DataException("Unexpected type:" + inputColumn.getType());
                    }

                    // Overwrite the column if it's base58 column.
                    Base58Column base58Column = base58ColumnMap.get(inputColumn.getName());
                    if (base58Column != null) {
                        final Column outputColumn = outputColumnMap.get(base58Column.getNewName().or(inputColumn.getName()));
                        final String convertedValue = convertValue(inputValue.toString(), base58Column.getIsEncode().get(), base58Column.getPrefix().get());
                        builder.setString(outputColumn, convertedValue);
                    }
                }
            }

            private String convertValue(String value, Boolean isEncode, String prefix) {
                String result = null;
                try {
                    if (isEncode) {
                        result = Base58.encodeWithPrefix(value, prefix);
                    } else {
                        result = Base58.decodeWithPrefix(value, prefix);
                    }
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
                return result;
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

    private static Map<String, Base58Column> convertBase58ColumnListToMap(List<Base58Column> base58Columns) {
        Map<String, Base58Column> result = new HashMap<>();
        for (Base58Column base58Column : base58Columns) {
            result.put(base58Column.getName(), base58Column);
        }
        return result;
    }

    private static Map<String, Column> convertColumnListToMap(List<Column> columns) {
        Map<String, Column> result = new HashMap<>();
        for (Column column : columns) {
            result.put(column.getName(), column);
        }
        return result;
    }
}
