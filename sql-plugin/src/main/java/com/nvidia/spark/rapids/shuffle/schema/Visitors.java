package com.nvidia.spark.rapids.shuffle.schema;

import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Visitors {
    public static <T, R> R visitSchema(Schema schema, SchemaVisitor<T, R> visitor) {
        Objects.requireNonNull(schema, "schema cannot be null");
        Objects.requireNonNull(visitor, "visitor cannot be null");

        List<T> childrenResult = new ArrayList<>(schema.getNumChildren());
        for (int i=0; i<schema.getNumChildren(); i++) {
            childrenResult.add(visitSchemaInner(schema.getChild(i), visitor));
        }

        return visitor.visitTopSchema(schema, childrenResult);
    }

    private static <T, R> T visitSchemaInner(Schema schema, SchemaVisitor<T, R> visitor) {
        switch (schema.getType().getTypeId()) {
            case STRUCT:
                List<T> children = IntStream.range(0, schema.getNumChildren())
                        .mapToObj(childIdx -> visitSchemaInner(schema.getChild(childIdx), visitor))
                        .collect(Collectors.toList());
                return visitor.visitStruct(schema, children);
            case LIST:
                T preVisitResult = visitor.preVisitList(schema);
                T childResult = visitSchemaInner(schema.getChild(0), visitor);
                return visitor.visitList(schema, preVisitResult, childResult);
            default:
                return visitor.visit(schema);
        }
    }


    /**
     * Entry point for visiting a schema with columns.
     */
    public static <T> void visitColumns(HostColumnVector[] cols,
        HostColumnsVisitor<T> visitor) {
        Objects.requireNonNull(cols, "cols cannot be null");
        Objects.requireNonNull(visitor, "visitor cannot be null");

        for (HostColumnVector col : cols) {
            visitSchema(col, visitor);
        }
    }

    private static <T> T visitSchema(HostColumnVectorCore col, HostColumnsVisitor<T> visitor) {
        switch (col.getType().getTypeId()) {
            case STRUCT:
                List<T> children = IntStream.range(0, col.getNumChildren())
                        .mapToObj(childIdx -> visitSchema(col.getChildColumnView(childIdx), visitor))
                        .collect(Collectors.toList());
                return visitor.visitStruct(col, children);
            case LIST:
                T preVisitResult = visitor.preVisitList(col);
                T childResult = visitSchema(col.getChildColumnView(0), visitor);
                return visitor.visitList(col, preVisitResult, childResult);
            default:
                return visitor.visit(col);
        }
    }
}
