/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;

/**
 * Type strategy that returns the quotient of an exact numeric division that includes at least one
 * decimal.
 */
public class DateFormatInputTypeStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.of(2);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final LogicalType firstType = argumentDataTypes.get(0).getLogicalType();
        if(firstType.is(LogicalTypeRoot.DATE) || firstType.isAnyOf(
                LogicalTypeFamily.CHARACTER_STRING,
                LogicalTypeFamily.TIMESTAMP)) {
            return callContext.fail(
                    throwOnFailure,
                    "EXTRACT requires 1st argument to be a date、string or timestamp type, "
                            + "but type is %s",
                    firstType);
        }
        final LogicalType secondType = argumentDataTypes.get(1).getLogicalType();
        if (secondType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return callContext.fail(
                    throwOnFailure,
                    "EXTRACT requires 2nd argument to be a string type, but type is %s",
                    secondType);
        }
        return Optional.of(argumentDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return null;
    }
}
