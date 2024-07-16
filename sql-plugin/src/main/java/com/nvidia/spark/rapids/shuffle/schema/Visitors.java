package com.nvidia.spark.rapids.shuffle.schema;

import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.Schema;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Visitors {
    public static <T, R> R visitSchema(Schema schema, SchemaVisitor<T, R> visitor) {
        Objects.requireNonNull(schema, "schema cannot be null");
        Objects.requireNonNull(visitor, "visitor cannot be null");

        List<T> childrenResult = IntStream.range(0, schema.getNumChildren())
                .mapToObj(i -> visitSchemaInner(schema.getChild(i), visitor))
                .collect(Collectors.toList());

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
    public static <T, R> R visitSchemaWithColumns(Schema schema, List<HostColumnVector> cols,
                                                  SchemaWithColumnsVisitor<T, R> visitor) {
        Objects.requireNonNull(schema, "schema cannot be null");
        Objects.requireNonNull(cols, "cols cannot be null");
        Objects.requireNonNull(visitor, "visitor cannot be null");

        if (schema.getNumChildren() != cols.size()) {
            throw new IllegalArgumentException("Schema children num: " + schema.getNumChildren() +
                    " is not same as columns num: " + cols.size());
        }

        List<T> childrenResult = IntStream.range(0, schema.getNumChildren())
                .mapToObj(i -> visitSchema(schema.getChild(i), cols.get(i), visitor))
                .collect(Collectors.toList());

        return visitor.visitTopSchema(schema, childrenResult);
    }

    private static <T, R> T visitSchema(Schema schema, HostColumnVectorCore col, SchemaWithColumnsVisitor<T, R> visitor) {
        switch (schema.getType().getTypeId()) {
            case STRUCT:
                List<T> children = IntStream.range(0, schema.getNumChildren())
                        .mapToObj(childIdx -> visitSchema(schema.getChild(childIdx), col.getChildColumnView(childIdx), visitor))
                        .collect(Collectors.toList());
                return visitor.visitStruct(schema, col, children);
            case LIST:
                T preVisitResult = visitor.preVisitList(schema, col);
                T childResult = visitSchema(schema.getChild(0), col.getChildColumnView(0), visitor);
                return visitor.visitList(schema, col, preVisitResult, childResult);
            default:
                return visitor.visit(schema, col);
        }
    }
}
