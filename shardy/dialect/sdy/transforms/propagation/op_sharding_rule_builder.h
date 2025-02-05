/* Copyright 2024 The Shardy Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef SHARDY_DIALECT_SDY_TRANSFORMS_PROPAGATION_OP_SHARDING_RULE_BUILDER_H_
#define SHARDY_DIALECT_SDY_TRANSFORMS_PROPAGATION_OP_SHARDING_RULE_BUILDER_H_

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>

#include "mlir/IR/BuiltinTypeInterfaces.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/Support/LLVM.h"
#include "shardy/dialect/sdy/ir/dialect.h"

namespace mlir {
namespace sdy {

// Represents a null dimension to indicate that a tensor shouldn't be mapped to
// a certain factor.
const int kNullDim = -1;

// Represents the type of a factor.
enum class FactorType {
  // The default type, containing the pass-through factors and other unset
  // factors.
  kDefault,

  // If we have sharding along reduction dimensions, the partitioner will add
  // all-reduce operations.
  kReduction,

  // If we have sharding along a dimension that needs replication, the
  // partitioner will make this dimension replicated.
  kNeedReplication,

  // If we have sharding along a dimension that has different sizes in different
  // tensors, the partitioner will add collective-permute operations.
  kSizeMismatch,
};

// The factor mappings that compose a dimension of a tensor.
struct DimMapping {
  SmallVector<int64_t> factorIndices;
};

// A list of mappings per dimension.
using TensorMapping = SmallVector<DimMapping>;

// A builder that helps incrementally create an `OpShardingRuleAttr`. See the
// definition of `OpShardingRule` for what it does/specifies.
class OpShardingRuleBuilder {
 public:
  explicit OpShardingRuleBuilder(
      TypeRange operandTypes, TypeRange resultTypes, MLIRContext* context,
      std::optional<int64_t> reserveNumFactors = std::nullopt);

  explicit OpShardingRuleBuilder(
      Operation* op, std::optional<int64_t> reserveNumFactors = std::nullopt);

  // Builds the `OpShardingRuleAttr`.
  //
  // Since all dimensions must have at least one factor, this method will add a
  // factor of size 1 to all dimensions that don't have a factor. This is done
  // in place for `factorSizes`, hence this method is not const, however the
  // additional factor sizes are removed after `OpShardingRuleAttr` is created,
  // so the builder is unchanged.
  OpShardingRuleAttr build();

  // Generic builder for any pointwise op (e.g. tanh, add, and, ceiling, etc.)
  static OpShardingRuleAttr buildPointwise(Operation* op);

  // Adds a new factor of size `factorSize` and type `factorType`, and maps it
  // to the corresponding dimension of each operand/result as specified by
  // `operandDims` and `resultDims`.
  //
  // Skips operands and results with corresponding dimension `kNullDim`.
  OpShardingRuleBuilder& addFactor(
      ArrayRef<int64_t> operandDims, ArrayRef<int64_t> resultDims,
      int64_t factorSize, FactorType factorType = FactorType::kDefault);

  // Same as addFactor above, but updates the same dimension for all operands
  // and results that have rank at least 1.
  //
  // Useful when creating rules for pointwise ops.
  OpShardingRuleBuilder& addFactor(
      int64_t dim, int64_t factorSize,
      FactorType factorType = FactorType::kDefault);

  // Adds a pointwise factor for all dimensions of all operands/results that
  // have rank at least 1. The factor type is determined by `predFactorType`.
  OpShardingRuleBuilder& addPointwise(
      ArrayRef<int64_t> shape,
      std::function<FactorType(int64_t)> getFactorType = [](int64_t) {
        return FactorType::kDefault;
      });

  // Adds a pointwise factor for all dimensions that satisfy `pred` of all
  // operands/results that have rank at least 1. The factor type is determined
  // by `predFactorType`.
  OpShardingRuleBuilder& addPointwiseIf(
      ArrayRef<int64_t> shape, std::function<bool(int64_t)> pred,
      std::function<FactorType(int64_t)> getFactorType = [](int64_t) {
        return FactorType::kDefault;
      });

  // Adds a pointwise factor for the matching dimensions and calls
  // `onMismatchFn` for the mismatching ones. A dimension is matching if (1)
  // the dimension size in `inShape` and `outShape` is the same, OR (2)
  // `alwaysAddFactor` is true.
  //
  // If `inShape` and `outShape` are empty, this method does nothing.
  OpShardingRuleBuilder& addPointwiseIfDimSizesMatch(
      ArrayRef<int64_t> inShape, ArrayRef<int64_t> outShape,
      bool alwaysAddFactor = false,
      std::function<void(int64_t dim, OpShardingRuleBuilder& builder)>
          onMismatchFn = [](int64_t dim, OpShardingRuleBuilder& builder) {});

 private:
  void updateFactorType(FactorType factorType, int64_t factorIndex);

  MLIRContext* context;
  SmallVector<int64_t> factorSizes;
  // The mappings of factor sizes for each operand/result. Specify the index of
  // the factor, with its corresponding size stored in `factorSizes`.
  SmallVector<TensorMapping> operandMappings;
  SmallVector<TensorMapping> resultMappings;

  SmallVector<int64_t> reductionFactors;
  SmallVector<int64_t> needReplicationFactors;
  SmallVector<int64_t> sizeMismatchFactors;
};

// Creates an identity mapping for an op with `numOperands` operands and
// `numResults` results, all with tensors of type `type`.
//
// Think of this as a pointwise op like add, but with many operands/results,
// i.e., all operands/results have the same mapping.
//
// NOTE: an empty rule {([])->([])} will be created for scalar ops.
OpShardingRuleAttr createIdentityShardingRule(ShapedType type,
                                              size_t numOperands = 1,
                                              size_t numResults = 1);

}  // namespace sdy
}  // namespace mlir

#endif  // SHARDY_DIALECT_SDY_TRANSFORMS_PROPAGATION_OP_SHARDING_RULE_BUILDER_H_
