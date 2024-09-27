package com.nvidia.spark.rapids.shuffle.schema;

import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.Schema;

import java.util.List;

public interface SchemaWithColumnsVisitor<T, R> {
    R visitTopSchema(Schema schema, List<T> children);

    T visitStruct(Schema structType, HostColumnVectorCore col, List<T> children);

    T preVisitList(Schema listType, HostColumnVectorCore col);
    T visitList(Schema listType, HostColumnVectorCore col, T preVisitResult, T childResult);

    T visit(Schema primitiveType, HostColumnVectorCore col);
}
