package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Schema;
import com.nvidia.spark.rapids.shuffle.schema.SchemaVisitor;

import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

import static com.nvidia.spark.rapids.shuffle.TableUtils.ensure;
import static com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer.padFor64byteAlignment;
import static com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer.safeLongToInt;

public abstract class MultiTableVisitor<T, R> implements SchemaVisitor<T, R> {
    private final List<SerializedTable> tables;
    private final long[] currentDataOffset;
    private final Deque<SliceInfo>[] sliceInfoStack;
    private final Deque<Long> totalRowCountStack;
    private final ColumnBufferProvider[] columns;
    // A temporary variable to keep if current column has null
    private boolean hasNull;
    private int currentIdx;
    // Temporary buffer to store data length of string column to avoid repeated allocation
    private final int[] strDataLen;
    // Temporary variable to calcluate total data length of string column
    private long totalStrDataLen;

    protected MultiTableVisitor(List<SerializedTable> inputTables) {
        Objects.requireNonNull(inputTables, "tables cannot be null");
        ensure(!inputTables.isEmpty(), "tables cannot be empty");
        this.tables = inputTables instanceof ArrayList ? inputTables : new ArrayList<>(inputTables);
        this.currentDataOffset = new long[tables.size()];
        this.sliceInfoStack = new Deque[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            SerializedTableHeader header = tables.get(i).getHeader();
            this.sliceInfoStack[i] = new ArrayDeque<>(16);
            this.sliceInfoStack[i].add(new SliceInfo(header.getOffset(), header.getNumRows()));
        }
        long totalRowCount = tables.stream().mapToLong(t -> t.getHeader().getNumRows()).sum();
        this.totalRowCountStack = new ArrayDeque<>(16);
        totalRowCountStack.addLast(totalRowCount);
        this.hasNull = true;
        this.currentIdx = 0;
        this.strDataLen = new int[tables.size()];
        this.totalStrDataLen = 0;
    }

    List<SerializedTable> getTables() {
        return tables;
    }

    @Override
    public R visitTopSchema(Schema schema, List<T> children) {
        return doVisitTopSchema(schema, children);
    }

    protected abstract R doVisitTopSchema(Schema schema, List<T> children);

    @Override
    public T visitStruct(Schema structType, List<T> children) {
        updateHasNull();
        T t = doVisitStruct(structType, children);
        updateOffsets(false, false, false, -1);
        currentIdx += 1;
        return t;
    }

    protected abstract T doVisitStruct(Schema structType, List<T> children);

    @Override
    public T preVisitList(Schema listType) {
        updateHasNull();
        T t = doPreVisitList(listType);
        updateOffsets(true, false, true, Integer.BYTES);
        currentIdx += 1;
        return t;
    }

    protected abstract T doPreVisitList(Schema listType);

    @Override
    public T visitList(Schema listType, T preVisitResult, T childResult) {
        T t = doVisitList(listType, preVisitResult, childResult);
        for (int tableIdx = 0; tableIdx < tables.size(); tableIdx++) {
            sliceInfoStack[tableIdx].removeLast();
        }
        totalRowCountStack.removeLast();
        return t;
    }

    protected abstract T doVisitList(Schema listType, T preVisitResult, T childResult);

    @Override
    public T visit(Schema primitiveType) {
        updateHasNull();
        if (primitiveType.getType().hasOffsets()) {
            // string type
            updateDataLen();
        }

        T t = doVisit(primitiveType);
        if (primitiveType.getType().hasOffsets()) {
            updateOffsets(true, true, false, -1);
        } else {
            updateOffsets(false, true, false, primitiveType.getType().getSizeInBytes());
        }
        currentIdx += 1;
        return t;
    }

    protected abstract T doVisit(Schema primitiveType);

    private void updateHasNull() {
        hasNull = false;
        for (SerializedTable table : tables) {
            if (table.getHeader().hasValidityBuffer(currentIdx)) {
                hasNull = true;
                return;
            }
        }
    }

    // For string column only
    private void updateDataLen() {
        totalStrDataLen = 0;
        // String's data len needs to be calculated from offset buffer
        for (int tableIdx = 0; tableIdx < getTableSize(); tableIdx += 1) {
            SliceInfo sliceInfo = sliceInfoOf(tableIdx);
            if (sliceInfo.getRowCount() > 0) {
                IntBuffer offsetBuffer = offsetBufferOf(tableIdx);
                int offset = offsetBuffer.get(0);
                int endOffset = offsetBuffer.get(safeLongToInt(sliceInfo.getRowCount()));

                strDataLen[tableIdx] = endOffset - offset;
                totalStrDataLen += strDataLen[tableIdx];
            } else {
                strDataLen[tableIdx] = 0;
            }
        }
    }

    private void updateOffsets(boolean updateOffset, boolean updateData, boolean updateSliceInfo, int sizeInBytes) {
        long totalRowCount = 0;
        for (int tableIdx = 0; tableIdx < tables.size(); tableIdx++) {
            SliceInfo sliceInfo = sliceInfoOf(tableIdx);
            if (sliceInfo.getRowCount() > 0) {
                if (updateSliceInfo) {
                    IntBuffer offsetBuffer = offsetBufferOf(tableIdx);
                    int startOffset = offsetBuffer.get(0);
                    int endOffset = offsetBuffer.get(safeLongToInt(sliceInfo.getRowCount()));
                    int rowCount = endOffset - startOffset;
                    totalRowCount += rowCount;

                    sliceInfoStack[tableIdx].addLast(new SliceInfo(startOffset, rowCount));
                }

                if (tables.get(tableIdx).getHeader().hasValidityBuffer(currentIdx)) {
                    currentValidityOffsets[tableIdx] += padFor64byteAlignment(sliceInfo.getValidityBufferInfo().getBufferLength());
                }

                if (updateOffset) {
                    currentOffsetOffsets[tableIdx] += padFor64byteAlignment((sliceInfo.getRowCount() + 1) * Integer.BYTES);
                    if (updateData) {
                        // string type
                        currentDataOffset[tableIdx] += padFor64byteAlignment(strDataLen[tableIdx]);
                    }
                    // otherwise list type
                } else {
                    if (updateData) {
                        // primitive type
                        currentDataOffset[tableIdx] += padFor64byteAlignment(sliceInfo.getRowCount() * sizeInBytes);
                    }
                }

            } else {
                if (updateSliceInfo) {
                    sliceInfoStack[tableIdx].addLast(new SliceInfo(0, 0));
                }
            }
        }

        if (updateSliceInfo) {
            totalRowCountStack.addLast(totalRowCount);
        }
    }

    // Below parts are information about current column

    protected long getTotalRowCount() {
        return totalRowCountStack.getLast();
    }


    protected boolean hasNull() {
        return hasNull;
    }

    protected SliceInfo sliceInfoOf(int tableIdx) {
        return sliceInfoStack[tableIdx].getLast();
    }

    protected IntBuffer offsetBufferOf(int tableIdx) {
        long startOffset = currentOffsetOffsets[tableIdx];
        int length = safeLongToInt((sliceInfoOf(tableIdx).getRowCount() + 1) * Integer.BYTES);
        return tables.get(tableIdx)
                .getBuffer()
                .asByteBuffer(startOffset, length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asIntBuffer();
    }

    protected HostMemoryBuffer validityBufferOf(int tableIdx) {
        if (tables.get(tableIdx).getHeader().hasValidityBuffer(currentIdx)) {
            long startOffset = currentValidityOffsets[tableIdx];
            long length = sliceInfoOf(tableIdx).getValidityBufferInfo().getBufferLength();
            return tables.get(tableIdx).getBuffer().slice(startOffset, length);
        } else {
            return null;
        }
    }

//    protected HostMemoryBuffer dataBufferOf(int tableIdx, int dataLen) {
//        long startOffset = currentDataOffset[tableIdx];
//        return tables.get(tableIdx).getBuffer().slice(startOffset, dataLen);
//    }

    protected void copyDataBuffer(HostMemoryBuffer dst, long dstOffset, int tableIdx, int dataLen) {
        long startOffset = currentDataOffset[tableIdx];
        HostMemoryBuffer src = tables.get(tableIdx).getBuffer();
        dst.copyFromHostBuffer(dstOffset, src, startOffset, dataLen);
    }

    protected long getTotalStrDataLen() {
        return totalStrDataLen;
    }

    protected int getStrDataLenOf(int tableIdx) {
        return strDataLen[tableIdx];
    }

    protected int getCurrentIdx() {
        return currentIdx;
    }

    public int getTableSize() {
        return this.tables.size();
    }
}
