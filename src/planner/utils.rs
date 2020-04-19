// Copyright 2020 Sujith Jay Nair
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes::*;

use crate::planner::logicalplan::*;

/// Create field meta-data from an expression, for use in a result set schema
pub fn expr_to_field(e: &Expression, input_schema: &Schema) -> Result<Field> {
    match e {
        Expression::UnresolvedColumn(name) => Ok(input_schema.column_with_name(&name).unwrap().1.clone()),
        Expression::Column(i) => {
            let input_schema_field_count = input_schema.fields().len();
            if *i < input_schema_field_count {
                Ok(input_schema.fields()[*i].clone())
            } else {
                Err(anyhow!(
                    "Column index {} out of bounds for input schema with {} field(s)",
                    *i, input_schema_field_count
                ))
            }
        }
        Expression::Literal(ref lit) => Ok(Field::new("lit", lit.get_datatype(), true)),
        Expression::ScalarFunction {
            ref name,
            ref return_type,
            ..
        } => Ok(Field::new(&name, return_type.clone(), true)),
        Expression::Cast { ref data_type, .. } => {
            Ok(Field::new("cast", data_type.clone(), true))
        }
        Expression::BinaryExpression {
            ref left,
            ref right,
            ..
        } => {
            let left_type = left.get_type(input_schema.clone())?;
            let right_type = right.get_type(input_schema.clone())?;
            Ok(Field::new(
                "binary_expr",
                DataType::Boolean,
                true,
            ))
        }
        _ => Err(anyhow!(
            "Cannot determine schema type for expression"
        )),
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields(expr: &Vec<Expression>, input_schema: &Schema) -> Result<Vec<Field>> {
    expr.iter()
        .map(|e| expr_to_field(e, input_schema))
        .collect()
}