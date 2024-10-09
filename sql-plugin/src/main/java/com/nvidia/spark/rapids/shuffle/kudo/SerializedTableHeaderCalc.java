package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.BufferType;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.Schema;
import com.nvidia.spark.rapids.shuffle.schema.SchemaWithColumnsVisitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer.padFor64byteAlignment;


class SerializedTableHeaderCalc implements SchemaWithColumnsVisitor<Void, SerializedTableHeader> {
    private final SliceInfo root;
    private final List<Boolean> hasValidityBuffer = new ArrayList<>(1024);
    private long totalDataLen;

    private Deque<SliceInfo> sliceInfos = new ArrayDeque<>();

    SerializedTableHeaderCalc(long rowOffset, long numRows) {
        this.root = new SliceInfo(rowOffset, numRows);
        this.totalDataLen = 0;
        sliceInfos.addLast(this.root);
    }

    @Override
    public SerializedTableHeader visitTopSchema(Schema schema, List<Void> children) {
        byte[] hasValidityBuffer = new byte[this.hasValidityBuffer.size()];
        for (int i = 0; i < this.hasValidityBuffer.size(); i++) {
            hasValidityBuffer[i] = (byte) (this.hasValidityBuffer.get(i) ? 1 : 0);
        }
        return new SerializedTableHeader(root.offset, root.rowCount,
                totalDataLen, hasValidityBuffer);
    }

    @Override
    public Void visitStruct(Schema structType, HostColumnVectorCore col, List<Void> children) {
        SliceInfo parent = sliceInfos.getLast();

        long validityBufferLength = 0;
        if (col.hasValidityVector()) {
            validityBufferLength = padFor64byteAlignment(parent.getValidityBufferInfo().getBufferLength());
        }


        totalDataLen += validityBufferLength;
        hasValidityBuffer.add(col.getValidity() != null);
        return null;
    }

    @Override
    public Void preVisitList(Schema listType, HostColumnVectorCore col) {
        SliceInfo parent = sliceInfos.getLast();


        long validityBufferLength = 0;
        if (col.hasValidityVector() && parent.rowCount > 0) {
            validityBufferLength = padFor64byteAlignment(parent.getValidityBufferInfo().getBufferLength());
        }

        long offsetBufferLength = 0;
        if (col.getOffsets() != null && parent.rowCount > 0) {
            offsetBufferLength = padFor64byteAlignment((parent.rowCount + 1) * Integer.BYTES);
        }

        this.totalDataLen += validityBufferLength + offsetBufferLength;

        hasValidityBuffer.add(col.getValidity() != null);

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
    public Void visitList(Schema listType, HostColumnVectorCore col, Void preVisitResult, Void childResult) {
        sliceInfos.removeLast();

        return null;
    }


    @Override
    public Void visit(Schema primitiveType, HostColumnVectorCore col) {
        SliceInfo parent = sliceInfos.peekLast();
        long validityBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.VALIDITY, parent);
        long offsetBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.OFFSET, parent);
        long dataBufferLen = calcPrimitiveDataLen(primitiveType, col, BufferType.DATA, parent);

        this.totalDataLen += validityBufferLen + offsetBufferLen + dataBufferLen;

        hasValidityBuffer.add(col.getValidity() != null);

        return null;
    }

    private long calcPrimitiveDataLen(Schema primitiveType,
                                      HostColumnVectorCore col,
                                      BufferType bufferType,
                                      SliceInfo info) {
        switch (bufferType) {
            case VALIDITY:
                if (col.hasValidityVector() && info.getRowCount() > 0) {
                    return  padFor64byteAlignment(info.getValidityBufferInfo().getBufferLength());
                } else {
                    return 0;
                }
            case OFFSET:
                if (DType.STRING.equals(primitiveType.getType()) && info.getRowCount() > 0) {
                    return padFor64byteAlignment((info.rowCount + 1) * Integer.BYTES);
                } else {
                    return 0;
                }
            case DATA:
                if (DType.STRING.equals(primitiveType.getType())) {
                    if (col.getOffsets() != null) {
                        long startByteOffset = col.getOffsets().getInt(info.offset * Integer.BYTES);
                        long endByteOffset = col.getOffsets().getInt((info.offset + info.rowCount) * Integer.BYTES);
                        return padFor64byteAlignment(endByteOffset - startByteOffset);
                    } else {
                        return 0;
                    }
                } else {
                    if (primitiveType.getType().getSizeInBytes() > 0) {
                        return padFor64byteAlignment(primitiveType.getType().getSizeInBytes() * info.rowCount);
                    } else {
                        return 0;
                    }
                }
            default:
                throw new IllegalArgumentException("Unexpected buffer type: " + bufferType);

        }
    }
}
