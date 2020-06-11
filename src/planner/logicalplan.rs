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
use std::fmt;
use std::sync::Arc;

use crate::parser::FileType;
use crate::planner::utils;


/// A Relation Expression
#[derive(Debug, Clone, PartialEq)]
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

    pub fn eq(&self, other: &Expression) -> Expression {
        Expression::BinaryExpression {
            left: Arc::new(self.clone()),
            op: Operator::Eq,
            right: Arc::new(other.clone()),
        }
    }
}

/// Create a column expression based on a column index
pub fn col_index(index: usize) -> Expression {
    Expression::Column(index)
}

/// Create a column expression based on a column name
pub fn col(name: &str) -> Expression {
    Expression::UnresolvedColumn(name.to_owned())
}

/// Create a literal string expression
pub fn lit_str(str: &str) -> Expression {
    Expression::Literal(ScalarValue::Utf8(str.to_owned()))
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
        expr: Vec<Expression>,
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
        /// Schema
        schema: Arc<Schema>,
    },
    /// A Selection
    Selection {
        /// The expression
        expr: Expression,
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        /// The sort expressions
        expr: Vec<Expression>,
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
        expr: Expression,
        /// The logical plan
        input: Arc<LogicalPlan>,
        /// Schema 
        schema: Arc<Schema>,
    },
    /// Represents a create table expression.
    CreateTable {
        /// The table name
        name: String,
        /// The table schema
        schema: Arc<Schema>,
        /// If the table is external
        external: bool,
        /// The file type of physical file
        file_type: Option<FileType>,
        /// The physical location
        location: Option<String>,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Arc<Schema> {
        match self {
            LogicalPlan::EmptyRelation { schema } => &schema,
            LogicalPlan::Scan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::Projection { schema, .. } => &schema,
            LogicalPlan::Selection { input, .. } => input.schema(),
            LogicalPlan::Sort { schema, .. } => &schema,
            LogicalPlan::Limit { schema, .. } => &schema,
            LogicalPlan::CreateTable { schema, .. } => &schema,
        }
    }
}

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match *self {
            LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
            LogicalPlan::Scan {
                ref table_name,
                ref projection,
                ..
            } => write!(f, "Scan: {} projection={:?}", table_name, projection),
            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Projection: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Selection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Selection: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Sort {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Sort: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Limit: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::CreateTable { ref name, .. } => {
                write!(f, "CreateTable: {:?}", name)
            }
        }
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

/// Builder for logical plans
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: &LogicalPlan) -> Self {
        Self { plan: plan.clone() }
    }

    /// Create an empty relation
    pub fn empty() -> Self {
        Self::from(&LogicalPlan::EmptyRelation {
            schema: Arc::new(Schema::empty()),
        })
    }

    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        table_name: &str,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = projection.clone().map(|p| {
            Schema::new(p.iter().map(|i| table_schema.field(*i).clone()).collect())
        });
        Ok(Self::from(&LogicalPlan::Scan {
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
            table_schema: Arc::new(table_schema.clone()),
            projected_schema: Arc::new(
                projected_schema.or(Some(table_schema.clone())).unwrap(),
            ),
            projection,
        }))
    }

    /// Apply a projection
    pub fn project(&self, expr: Vec<Expression>) -> Result<Self> {
        let input_schema = self.plan.schema();
        let projected_expr = if expr.contains(&Expression::Wildcard) {
            let mut expr_vec = vec![];
            (0..expr.len()).for_each(|i| match &expr[i] {
                Expression::Wildcard => {
                    (0..input_schema.fields().len())
                        .for_each(|i| expr_vec.push(col_index(i).clone()));
                }
                _ => expr_vec.push(expr[i].clone()),
            });
            expr_vec
        } else {
            expr.clone()
        };

        let schema = Schema::new(utils::exprlist_to_fields(
            &projected_expr,
            input_schema.as_ref(),
        )?);

        Ok(Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Arc::new(self.plan.clone()),
            schema: Arc::new(schema),
        }))
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expression) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Selection {
            expr,
            input: Arc::new(self.plan.clone()),
        }))
    }

    /// Apply a limit
    pub fn limit(&self, expr: Expression) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Limit {
            expr,
            input: Arc::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        }))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expression>) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Sort {
            expr,
            input: Arc::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        }))
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_simple_plan_builder() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "company",
            "employee",
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        .filter(col("state").eq(&lit_str("CO")))?
        .project(vec![col("id")])?
        .build()?;

        let expected = "Projection: UnresolvedColumn(\"id\")\
        \n  Selection: BinaryExpression { left: UnresolvedColumn(\"state\"), op: Eq, right: Literal(Utf8(\"CO\")) }\
        \n    Scan: employee projection=Some([0, 3])";

        assert_eq!(expected, format!("{:?}", plan));
        Ok(())

    }

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }
}
