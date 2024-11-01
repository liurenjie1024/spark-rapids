package com.nvidia.spark.rapids.shuffle.kudo;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Optional;

/**
 * Holds the metadata about a serialized table. If this is being read from a stream
 * isInitialized will return true if the metadata was read correctly from the stream.
 * It will return false if an EOF was encountered at the beginning indicating that
 * there was no data to be read.
 */
public final class SerializedTableHeader {
    /**
     * Magic number "KUDO" in ASCII.
     */
    private static final int SER_FORMAT_MAGIC_NUMBER = 0x4B55444F;
    private static final short VERSION_NUMBER = 0x0001;

    // Useful for reducing calculations in writing.
    private long offset;
    private long numRows;
    private long validityBufferLen;
    private long offsetBufferLen;
    private long totalDataLen;
    private int numColumns;
    private byte[] hasValidityBuffer;

    private boolean initialized = false;


    public SerializedTableHeader(DataInputStream din) throws IOException {
        readFrom(din);
    }

    SerializedTableHeader(long offset, long numRows, long validityBufferLen, long offsetBufferLen,
        long totalDataLen, int numColumns, byte[] hasValidityBuffer) {
        this.offset = offset;
        this.numRows = numRows;
        this.validityBufferLen = validityBufferLen;
        this.offsetBufferLen = offsetBufferLen;
        this.totalDataLen = totalDataLen;
        this.numColumns = numColumns;
        this.hasValidityBuffer = hasValidityBuffer;

        this.initialized = true;
    }

    /**
     * Returns the size of a buffer needed to read data into the stream.
     */
    public long getTotalDataLen() {
        return totalDataLen;
    }

    /**
     * Returns the number of rows stored in this table.
     */
    public long getNumRows() {
        return numRows;
    }

    public long getOffset() {
        return offset;
    }

    /**
     * Returns true if the metadata for this table was read, else false indicating an EOF was
     * encountered.
     */
    public boolean wasInitialized() {
        return initialized;
    }

    public boolean hasValidityBuffer(int columnIndex) {
        int pos = columnIndex / 8;
        int bit = columnIndex % 8;
        return (hasValidityBuffer[pos] & (1 << bit)) != 0;
    }

    public int getSerializedSize() {
        return 4 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + hasValidityBuffer.length;
    }

    public int getNumColumns() {
        return numColumns;
    }

    public long getValidityBufferLen() {
        return validityBufferLen;
    }

    public long getOffsetBufferLen() {
        return offsetBufferLen;
    }

    public boolean isInitialized() {
        return initialized;
    }

    private void readFrom(DataInputStream din) throws IOException {
        try {
            int num = din.readInt();
            if (num != SER_FORMAT_MAGIC_NUMBER) {
                throw new IllegalStateException("THIS DOES NOT LOOK LIKE CUDF SERIALIZED DATA. " + "Expected magic number " + SER_FORMAT_MAGIC_NUMBER + " Found " + num);
            }
        } catch (EOFException e) {
            // If we get an EOF at the very beginning don't treat it as an error because we may
            // have finished reading everything...
            return;
        }
        short version = din.readShort();
        if (version != VERSION_NUMBER) {
            throw new IllegalStateException("READING THE WRONG SERIALIZATION FORMAT VERSION FOUND " + version + " EXPECTED " + VERSION_NUMBER);
        }

        offset = din.readInt();
        numRows = din.readInt();

        validityBufferLen = din.readInt();
        offsetBufferLen = din.readInt();
        totalDataLen = din.readInt();
        numColumns = din.readInt();
        int validityBufferLength = din.readInt();
        hasValidityBuffer = new byte[validityBufferLength];
        din.readFully(hasValidityBuffer);

        initialized = true;
    }

    public void writeTo(DataWriter dout) throws IOException {
        // Now write out the data
        dout.writeInt(SER_FORMAT_MAGIC_NUMBER);
        dout.writeShort(VERSION_NUMBER);

        dout.writeInt((int)offset);
        dout.writeInt((int)numRows);
        dout.writeInt((int)validityBufferLen);
        dout.writeInt((int)offsetBufferLen);
        dout.writeInt((int)totalDataLen);
        dout.writeInt(numColumns);
        dout.writeInt(hasValidityBuffer.length);
        dout.write(hasValidityBuffer, 0, hasValidityBuffer.length);
    }

    @Override
    public String toString() {
        return "SerializedTableHeader{" +
            "offset=" + offset +
            ", numRows=" + numRows +
            ", validityBufferLen=" + validityBufferLen +
            ", offsetBufferLen=" + offsetBufferLen +
            ", totalDataLen=" + totalDataLen +
            ", numColumns=" + numColumns +
            ", hasValidityBuffer=" + Arrays.toString(hasValidityBuffer) +
            ", initialized=" + initialized +
            '}';
    }
}
