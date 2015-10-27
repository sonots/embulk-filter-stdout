package org.embulk.filter.stdout;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Page;
import org.embulk.spi.Exec;
import org.embulk.spi.util.PagePrinter;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.ColumnVisitor;

public class StdoutFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task, TimestampFormatter.FormatterTask
    {
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(final TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private final PagePrinter pagePrinter = new PagePrinter(inputSchema, task);
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
            }
        };
    }
}
