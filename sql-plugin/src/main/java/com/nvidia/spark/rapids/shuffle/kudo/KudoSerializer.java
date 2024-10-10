package com.nvidia.spark.rapids.shuffle.kudo;

import ai.rapids.cudf.BufferType;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.shuffle.TableUtils;
import com.nvidia.spark.rapids.shuffle.schema.Visitors;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.nvidia.spark.rapids.shuffle.TableUtils.withTime;

public class KudoSerializer {

  private static final byte[] PADDING = new byte[64];

  static {
    Arrays.fill(PADDING, (byte) 0);
  }

  public String version() {
    return "MultiTableSerializer-v7";
  }

  public long writeToStream(Table table, OutputStream out, long rowOffset, long numRows) {

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

  public long writeToStream(HostColumnVector[] columns, OutputStream out, long rowOffset, long numRows) {
    if (numRows < 0) {
      throw new IllegalArgumentException("numRows must be >= 0");
    }

    if (numRows == 0 || columns.length == 0) {
      return 0;
    }

    try {
      return writeSliced(columns, writerFrom(out), rowOffset, numRows);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long writeRowsToStream(OutputStream out, long numRows) {
    if (numRows <= 0) {
      throw new IllegalArgumentException("Number of rows must be > 0, but was " + numRows);
    }
    try {
      DataWriter writer = writerFrom(out);
      SerializedTableHeader header = new SerializedTableHeader(0, safeLongToInt(numRows), 0, 0, 0, new byte[0]);
      header.writeTo(writer);
      writer.flush();
      return header.getSerializedSize();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public SerializedTable readOneTableBuffer(InputStream in) {
    Objects.requireNonNull(in, "Input stream must not be null");

    try {
      DataInputStream din = readerFrom(in);
      SerializedTableHeader header = new SerializedTableHeader(din);
      if (!header.wasInitialized()) {
        return null;
      }

      if (header.getNumRows() <= 0) {
        throw new IllegalArgumentException("Number of rows must be > 0, but was " + header.getNumRows());
      }

      // Header only
      if (header.getNumColumns() == 0) {
        return new SerializedTable(header, null);
      }

      HostMemoryBuffer buffer = HostMemoryBuffer.allocate(header.getTotalDataLen(), false);
      RefUtils.copyFromStream(buffer, 0, din, header.getTotalDataLen());
      return new SerializedTable(header, buffer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Pair<HostMergeResult, MergeMetrics> mergeToHost(List<SerializedTable> serializedTables,
      Schema schema) {
    MergeMetrics.Builder metricsBuilder = MergeMetrics.builder();

    MergedInfoCalc mergedInfoCalc = withTime(() -> MergedInfoCalc.calc(schema, serializedTables),
        metricsBuilder::calcHeaderTime);
//            System.err.println("MergedInfoCalc: " + mergedInfoCalc);
    HostMergeResult result = withTime(() -> HostBufferMerger.merge(schema, mergedInfoCalc),
        metricsBuilder::mergeIntoHostBufferTime);
    return Pair.of(result, metricsBuilder.build());

  }

  public Pair<ContiguousTable, MergeMetrics> mergeTable(List<SerializedTable> buffers,
      Schema schema) {
    Pair<HostMergeResult, MergeMetrics> result = mergeToHost(buffers, schema);
    MergeMetrics.Builder builder = MergeMetrics.builder(result.getRight());
    try (HostMergeResult children = result.getLeft()) {
//            System.err.println("HostMergeResult: " + children);
      ContiguousTable table = withTime(() -> children.toContiguousTable(schema),
          builder::convertIntoContiguousTableTime);

      return Pair.of(table, builder.build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static long writeSliced(HostColumnVector[] columns, DataWriter out, long rowOffset, long numRows) throws Exception {
    List<HostColumnVector> columnList = Arrays.stream(columns).collect(Collectors.toList());

    Schema.Builder schemaBuilder = Schema.builder();
    for (int i = 0; i < columns.length; i++) {
      TableUtils.addToSchema(columns[i], "col_" + i + "_", schemaBuilder);
    }
    Schema schema = schemaBuilder.build();


    SerializedTableHeaderCalc headerCalc = new SerializedTableHeaderCalc(rowOffset, numRows);
    SerializedTableHeader header = Visitors.visitSchemaWithColumns(schema, columnList, headerCalc);
    header.writeTo(out);

    long bytesWritten = 0;
    for (BufferType bufferType : Arrays.asList(BufferType.VALIDITY, BufferType.OFFSET, BufferType.DATA)) {
      bytesWritten += Visitors.visitSchemaWithColumns(schema, columnList, new SlicedBufferSerializer(rowOffset, numRows, bufferType, out));
    }

    if (bytesWritten != header.getTotalDataLen()) {
      throw new IllegalStateException("Header total data length: " + header.getTotalDataLen() +
          " does not match actual written data length: " + bytesWritten +
          ", rowOffset: " + rowOffset + " numRows: " + numRows);
    }

    out.flush();

    return header.getSerializedSize() + bytesWritten;
  }

  private static DataInputStream readerFrom(InputStream in) {
    if (!(in instanceof DataInputStream)) {
      in = new DataInputStream(in);
    }
    return new DataInputStream(in);
  }

  private static DataWriter writerFrom(OutputStream out) {
    if (!(out instanceof DataOutputStream)) {
      out = new DataOutputStream(new BufferedOutputStream(out));
    }
    return new DataOutputStreamWriter((DataOutputStream) out);
  }


  /////////////////////////////////////////////
  // METHODS
  /////////////////////////////////////////////


  /////////////////////////////////////////////
// PADDING FOR ALIGNMENT
/////////////////////////////////////////////
  static long padFor64byteAlignment(long orig) {
    return ((orig + 63) / 64) * 64;
  }

  static long padFor64byteAlignment(DataWriter out, long bytes) throws IOException {
    final long paddedBytes = padFor64byteAlignment(bytes);
    if (paddedBytes > bytes) {
      out.write(PADDING, 0, (int) (paddedBytes - bytes));
    }
    return paddedBytes;
  }

  static int safeLongToInt(long value) {
//        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
//            throw new ArithmeticException("Overflow: long value is too large to fit in an int");
//        }
    return (int) value;
  }

}