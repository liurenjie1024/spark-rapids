package com.nvidia.spark.rapids.shuffle;

import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.IntStream;

public interface TableSerializer {
    /**
     * Get the version of the serializer format.
     * <p>
     * This method is mainly used for debugging and testing purposes.
     */
    String version();

    default long writeToStream(Table table, OutputStream out, long rowOffset, long numRows) {

        HostColumnVector[] columns = null;
        try {
            columns = IntStream.range(0, table.getNumberOfColumns())
                    .mapToObj(table::getColumn)
                    .map(ColumnView::copyToHost)
                    .toArray(HostColumnVector[]::new);
            return writeToStream(columns, out, rowOffset, numRows);
        } finally {
            if (columns != null) {
                for (HostColumnVector column : columns) {
                    column.close();
                }
            }
        }
    }

    long writeToStream(HostColumnVector[] columns, OutputStream out, long rowOffset,
                       long numRows);


    /**
     * Write a rowcount only header to the output stream in a case
     * where a columnar batch with no columns but a non zero row count is received
     * @param out the stream to write the serialized table out to.
     * @param numRows the number of rows to write out.
     */
    long writeRowsToStream(OutputStream out, long numRows);

    /**
     * Read a table from a stream, with given schema.
     *
     * @return An intermediate buffer of the table. We use object here since it's implementation specific. The
     * caller determines when to merge buffers.
     */
    Object readOneTableBuffer(InputStream in);

    /**
     * Merge multiple table buffers returned in {@link #readOneTableBuffer}  into a single table on gpu.
     *
     * @param buffers An array of table buffers.
     * @param schema  The schema of the table.
     * @return The merged table.
     */
    ContiguousTable mergeTable(List<Object> buffers, Schema schema);

    /**
     * Merge multiple serialized table buffers returned in {@link #readOneTableBuffer}  into a single table on host.
     *
     * @param buffers An array of table buffers.
     * @param schema  The schema of the table.
     * @return The merged table.
     */
    Object mergeToHost(List<Object> buffers, Schema schema);
}
