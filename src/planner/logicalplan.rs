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

use anyhow::Result;
use anyhow::anyhow;
use arrow::datatypes::{DataType, Schema};
use std::sync::Arc;


/// A Relation Expression
#[derive(Clone, PartialEq)]
pub enum Expression {
    /// Reference to column by name
    UnresolvedColumn(String),
    /// Index into a value within the row
    Column(usize),
    /// Literal value
    Literal(ScalarValue),
    /// Binary expression
    BinaryExpression {
        /// LHS of the binary expression
        left: Arc<Expression>,
        /// The comparison operator
        op: Operator,
        /// RHS of the binary expression
        right: Arc<Expression>,
    },
    /// The unary operator NOT
    Not(Arc<Expression>),
    /// The unary operator IS NOT NULL
    IsNotNull(Arc<Expression>),
    /// The unary operator IS NULL
    IsNull(Arc<Expression>),
    /// Cast the type of a value
    Cast {
        /// The expression to cast
        expr: Arc<Expression>,
        /// The type to cast into
        data_type: DataType,
    },
    /// Sort expression
    Sort {
        /// The expression to sort on
        expr: Arc<Expression>,
        /// Sort order
        asc: bool,
    },
    /// Scalar function
    ScalarFunction {
        /// Function name
        name: String,
        /// Functions arguments
        args: Vec<Expression>,
        /// The return-type of the function
        return_type: DataType,
    },
    /// Aggregation function
    AggregateFunction {
        /// Function name
        name: String,
        /// Functions arguments
        args: Vec<Expression>,
        /// The return-type of the function
        return_type: DataType,
    },
    /// Wildcard
    Wildcard,
}

impl Expression {
    pub fn get_type(&self, schema: Schema) -> Result<DataType> {
        match self {
            Expression::AggregateFunction{..} => Err(anyhow!("Aggregation is currently not supported.")),
            Expression::BinaryExpression{..} => Ok(DataType::Boolean),
            Expression::Cast{data_type, ..} => Ok(data_type.clone()),
            Expression::Column(idx) => Ok(schema.field(*idx).data_type().clone()),
            Expression::IsNotNull(_) => Ok(DataType::Boolean),
            Expression::IsNull(_) => Ok(DataType::Boolean),
            Expression::Literal(v) => Ok(v.get_datatype()),
            Expression::Not(_) => Ok(DataType::Boolean),
            Expression::ScalarFunction{return_type, .. } => Ok(return_type.clone()),
            Expression::Sort{ ref expr, .. } => expr.get_type(schema),
            Expression::UnresolvedColumn(name) => {
                Ok(schema.column_with_name(&name).unwrap().1.data_type().clone())
            },
            Expression::Wildcard => Err(anyhow!("Wildcards are invalid expressions in a Logical Plan.")),
            _ => Err(anyhow!("Expression not implemented in Mura.")),
        }
    }
}

/// Operators applied to Expressions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is less than right side
    Lt,
    /// Left side is less than or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater than or equal to right side
    GtEq,
    /// Logical AND
    And,
    /// Logical OR
    Or,
}

/// ScalarValue
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// null value
    Null,
    /// true or false value
    Boolean(bool),
    /// 32bit float
    Float32(f32),
    /// 64bit float
    Float64(f64),
    /// signed 8bit int
    Int8(i8),
    /// signed 16bit int
    Int16(i16),
    /// signed 32bit int
    Int32(i32),
    /// signed 64bit int
    Int64(i64),
    /// unsigned 8bit int
    UInt8(u8),
    /// unsigned 16bit int
    UInt16(u16),
    /// unsigned 32bit int
    UInt32(u32),
    /// unsigned 64bit int
    UInt64(u64),
    /// utf-8 encoded string
    Utf8(String),
    /// List of scalars packed as a struct
    Struct(Vec<ScalarValue>),
}

impl ScalarValue {
    
    pub fn get_datatype(&self) -> DataType {
        match *self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            _ => panic!("Cannot treat {:?} as scalar value", self),
        }
    }
}

/// LogicalPlan represents different types of relations.
#[derive(Clone)]
pub enum LogicalPlan {
    /// A Projection
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
        /// Schema
        schema: Arc<Schema>,
    },
    /// A Selection
    Selection {
        /// The expression
        expr: Expr,
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
        /// Schema
        schema: Arc<Schema>,
    },
    /// A scan against a catalog table
    Scan {
        /// The name of the schema
        schema_name: String,
        /// The name of the table
        table_name: String,
        /// The table schema
        table_schema: Arc<Schema>,
        /// The schema, if projections are applied on scan
        projected_schema: Arc<Schema>,
        /// Projection columns on the scan
        projection: Option<Vec<usize>>,
    },
    /// An empty relation with an empty schema
    EmptyRelation {
        /// Schema
        schema: Arc<Schema>,
    },
    /// Represents the maximum number of records to return
    Limit {
        /// The expression
        expr: Expr,
        /// The logical plan
        input: Arc<LogicalPlan>,
        /// Schema 
        schema: Arc<Schema>,
    },
    /// Represents a create external table expression.
    CreateExternalTable {
        /// The table schema
        schema: Arc<Schema>,
        /// The table name
        name: String,
        /// The physical location
        location: String,
        /// The file type of physical file
        file_type: FileType,
        /// Whether the CSV file contains a header
        header_row: bool,
    },
}
