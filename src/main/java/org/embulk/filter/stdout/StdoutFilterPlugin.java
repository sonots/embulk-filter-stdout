package org.embulk.filter.stdout;

import java.util.Optional;

import org.embulk.spi.BufferAllocator;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Page;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.ColumnVisitor;
import org.embulk.util.config.TaskMapper;

public class StdoutFilterPlugin
        implements FilterPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
            .builder()
            .addDefaultModules()
            .build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    public interface PluginTask
            extends Task
    {
        @Config("timezone")
        @ConfigDefault("null")
        public Optional<String> getTimezone();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.toTaskSource(), outputSchema);
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
        final PluginTask task = taskMapper.map(taskSource, PluginTask.class);

        return new PageOutput() {
            private final PageReader pageReader = getPageReader(inputSchema);
            private final PageBuilder pageBuilder = getPageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final PagePrinter pagePrinter = new PagePrinter(inputSchema, task.getTimezone().orElse("UTC"));
            private final ColumnVisitorImpl visitor = new ColumnVisitorImpl(pageBuilder);

            @Override
            public void finish() {
                pageBuilder.finish();
            }

            @Override
            public void close() {
                pageBuilder.close();
            }

            @Override
            public void add(Page page) {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    System.out.println(pagePrinter.printRecord(pageReader, ",")); // here!
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }

            class ColumnVisitorImpl implements ColumnVisitor {
                private final PageBuilder pageBuilder;

                ColumnVisitorImpl(PageBuilder pageBuilder) {
                    this.pageBuilder = pageBuilder;
                }

                @Override
                public void booleanColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(outputColumn));
                    }
                }

                @Override
                public void longColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setLong(outputColumn, pageReader.getLong(outputColumn));
                    }
                }

                @Override
                public void doubleColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setDouble(outputColumn, pageReader.getDouble(outputColumn));
                    }
                }

                @Override
                public void stringColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setString(outputColumn, pageReader.getString(outputColumn));
                    }
                }

                @Override
                public void timestampColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(outputColumn));
                    }
                }

                @Override
                public void jsonColumn(Column outputColumn) {
                    if (pageReader.isNull(outputColumn)) {
                        pageBuilder.setNull(outputColumn);
                    } else {
                        pageBuilder.setJson(outputColumn, pageReader.getJson(outputColumn));
                    }
                }
            }
        };
    }

    @SuppressWarnings("deprecation")
    private static PageBuilder getPageBuilder(final BufferAllocator bufferAllocator, final Schema schema, final PageOutput output) {
        if (HAS_EXEC_GET_PAGE_BUILDER) {
            return Exec.getPageBuilder(bufferAllocator, schema, output);
        } else {
            return new PageBuilder(bufferAllocator, schema, output);
        }
    }

    @SuppressWarnings("deprecation")
    private static PageReader getPageReader(final Schema schema) {
        if (HAS_EXEC_GET_PAGE_READER) {
            return Exec.getPageReader(schema);
        } else {
            return new PageReader(schema);
        }
    }

    private static boolean hasExecGetPageReader() {
        try {
            Exec.class.getMethod("getPageReader", Schema.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static boolean hasExecGetPageBuilder() {
        try {
            Exec.class.getMethod("getPageBuilder", BufferAllocator.class, Schema.class, PageOutput.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static final boolean HAS_EXEC_GET_PAGE_READER = hasExecGetPageReader();
    private static final boolean HAS_EXEC_GET_PAGE_BUILDER = hasExecGetPageBuilder();
}
