package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;
import com.nvidia.spark.rapids.shuffle.schema.HostColumnsVisitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import static com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer.padForHostAlignment;


class SerializedTableHeaderCalc implements HostColumnsVisitor<Void> {
    private final SliceInfo root;
    private final BitSet bitset;
    private long validityBufferLen;
    private long offsetBufferLen;
    private long totalDataLen;
    private int nextColIdx;

    private Deque<SliceInfo> sliceInfos = new ArrayDeque<>();

    SerializedTableHeaderCalc(long rowOffset, long numRows, int numCols) {
        this.root = new SliceInfo(rowOffset, numRows);
        this.totalDataLen = 0;
        sliceInfos.addLast(this.root);
        this.bitset = new BitSet(numCols);
        this.nextColIdx = 0;
    }

    public SerializedTableHeader getHeader() {
        // Bitset calculates length based on the highest set bit, so we need to add 1 to ensure
        // that the length is correct.
        this.setHasValidity(true);
        return new SerializedTableHeader(root.offset, root.rowCount,
                validityBufferLen, offsetBufferLen,
                totalDataLen, nextColIdx, bitset.toLongArray());
    }

    @Override
    public Void visitStruct(HostColumnVectorCore col, List<Void> children) {
        SliceInfo parent = sliceInfos.getLast();

        long validityBufferLength = 0;
        if (col.hasValidityVector()) {
            validityBufferLength = padForHostAlignment(parent.getValidityBufferInfo().getBufferLength());
        }

        this.validityBufferLen += validityBufferLength;

        totalDataLen += validityBufferLength;
        this.setHasValidity(col.getValidity() != null);
        return null;
    }

    @Override
    public Void preVisitList(HostColumnVectorCore col) {
        SliceInfo parent = sliceInfos.getLast();


        long validityBufferLength = 0;
        if (col.hasValidityVector() && parent.rowCount > 0) {
            validityBufferLength = padForHostAlignment(parent.getValidityBufferInfo().getBufferLength());
        }

        long offsetBufferLength = 0;
        if (col.getOffsets() != null && parent.rowCount > 0) {
            offsetBufferLength = padForHostAlignment((parent.rowCount + 1) * Integer.BYTES);
        }

        this.validityBufferLen += validityBufferLength;
        this.offsetBufferLen += offsetBufferLength;
        this.totalDataLen += validityBufferLength + offsetBufferLength;

        this.setHasValidity(col.getValidity() != null);

        SliceInfo current;

        if (col.getOffsets() != null) {
            long start = col.getOffsets().getInt(parent.offset * Integer.BYTES);
            long end = col.getOffsets().getInt((parent.offset + parent.rowCount) * Integer.BYTES);
            long rowCount = end - start;
            current = new SliceInfo(start, rowCount);
        } else {
            current = new SliceInfo(0, 0);
        }

        sliceInfos.addLast(current);
        return null;
    }

    @Override
    public Void visitList(HostColumnVectorCore col, Void preVisitResult, Void childResult) {
        sliceInfos.removeLast();

        return null;
    }


    @Override
    public Void visit(HostColumnVectorCore col) {
        SliceInfo parent = sliceInfos.peekLast();
        long validityBufferLen = dataLenOfValidityBuffer(col, parent);
        long offsetBufferLen = dataLenOfOffsetBuffer(col, parent);
        long dataBufferLen = dataLenOfDataBuffer(col, parent);

        this.validityBufferLen += validityBufferLen;
        this.offsetBufferLen += offsetBufferLen;
        this.totalDataLen += validityBufferLen + offsetBufferLen + dataBufferLen;

        this.setHasValidity(col.getValidity() != null);

        return null;
    }

    private void setHasValidity(boolean hasValidityBuffer) {
        bitset.set(nextColIdx, hasValidityBuffer);
        nextColIdx++;
    }

    private static long dataLenOfValidityBuffer(HostColumnVectorCore col, SliceInfo info) {
        if (col.hasValidityVector() && info.getRowCount() > 0) {
            return  padForHostAlignment(info.getValidityBufferInfo().getBufferLength());
        } else {
            return 0;
        }
    }

    private static long dataLenOfOffsetBuffer(HostColumnVectorCore col, SliceInfo info) {
        if (DType.STRING.equals(col.getType()) && info.getRowCount() > 0) {
            return padForHostAlignment((info.rowCount + 1) * Integer.BYTES);
        } else {
            return 0;
        }
    }

    private static long dataLenOfDataBuffer(HostColumnVectorCore col, SliceInfo info) {
        if (DType.STRING.equals(col.getType())) {
            if (col.getOffsets() != null) {
                long startByteOffset = col.getOffsets().getInt(info.offset * Integer.BYTES);
                long endByteOffset = col.getOffsets().getInt((info.offset + info.rowCount) * Integer.BYTES);
                return padForHostAlignment(endByteOffset - startByteOffset);
            } else {
                return 0;
            }
        } else {
            if (col.getType().getSizeInBytes() > 0) {
                return padForHostAlignment(col.getType().getSizeInBytes() * info.rowCount);
            } else {
                return 0;
            }
        }
    }
}
