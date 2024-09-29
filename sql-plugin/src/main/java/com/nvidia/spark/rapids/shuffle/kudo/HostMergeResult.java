package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.shuffle.TableUtils;
import com.nvidia.spark.rapids.shuffle.schema.Visitors;

import java.util.List;

public class HostMergeResult implements AutoCloseable {
    private final List<ColumnViewInfo> columnOffsets;
    private final HostMemoryBuffer hostBuf;

    public HostMergeResult(HostMemoryBuffer hostBuf, List<ColumnViewInfo> columnOffsets) {
        this.columnOffsets = columnOffsets;
        this.hostBuf = hostBuf;
    }

    @Override
    public void close() throws Exception {
        if (hostBuf != null) {
            hostBuf.close();
        }
    }

    public ContiguousTable toContiguousTable(Schema schema) {
        return TableUtils.closeIfException(DeviceMemoryBuffer.allocate(hostBuf.getLength()),
            deviceMemBuf -> {
            if (hostBuf.getLength() > 0) {
                deviceMemBuf.copyFromHostBuffer(hostBuf);
            }

            TableBuilder builder = new TableBuilder(columnOffsets, deviceMemBuf);
            Table t = Visitors.visitSchema(schema, builder);

            return RefUtils.makeContiguousTable(t, deviceMemBuf);
        });
    }

    public long getDataLen() {
        if (hostBuf != null) {
            return hostBuf.getLength();
        } else {
            return 0L;
        }
    }

    @Override
    public String toString() {
        return "HostMergeResult{" +
                "columnOffsets=" + columnOffsets +
                ", hostBuf length =" + hostBuf.getLength() +
                '}';
    }
}
