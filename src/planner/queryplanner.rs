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
use arrow::datatypes::DataType;
use sqlparser::ast::*;
use std::sync::Arc;

use crate::parser::FileType;
use crate::planner::catalog::SchemaCatalog;
use crate::planner::logicalplan::*;

pub struct QueryPlanner{
    catalog: Arc<dyn SchemaCatalog>,
}

impl QueryPlanner {

    pub fn new(catalog: Arc<dyn SchemaCatalog>) -> Arc<QueryPlanner> {
        Arc::new(QueryPlanner { catalog })
    }

    pub fn to_logical_plan(&self, statements: &Vec<Statement>) -> Result<LogicalPlan> {
        match statements.get(0).unwrap() {
            Statement::Query(q) => {
                if !q.ctes.is_empty() {
                    return Err(anyhow!("CTEs are currently not supported."))
                    };
                if q.offset.is_some() {
                    return Err(anyhow!("Offset is currently not supported."))
                    };
                if q.fetch.is_some() {
                    return Err(anyhow!("Fetch is currently not supported."))
                    };
                
                match q.body {
                    SetExpr::Select(ref sel) => {
                        let table = &sel.from.get(0).unwrap().relation;
                        match table {
                            TableFactor::Table { name, alias, args, with_hints } => match self.catalog.fetch_table_info(&format!("{}", name)) {
                                    Some(schema) => Ok(LogicalPlanBuilder::scan(
                                        "default",
                                        &format!("{}", name),
                                        schema.as_ref(),
                                        None,
                                    )?
                                    .build()?),
                                    None => Err(anyhow!(
                                        "no schema found for table {}",
                                        name
                                    )),
                                },
                            _ => Err(anyhow!("Derived tables not implemented!")),
                        }
                    },
                    _ => Err(anyhow!("Not implemented!")), 
                }
            }

            Statement::CreateTable{
                name,
                columns,
                constraints,
                with_options,
                external,
                file_format,
                location,
            } => {
                let schema = Arc::new(self.build_schema(columns)?);
                let file_type = match *file_format {
                    Some(FileFormat::PARQUET) => Some(FileType::Parquet),
                    _ => None,
                };

                Ok(LogicalPlan::CreateTable {
                    name: name.to_string(),
                    schema: schema,
                    external: *external,
                    file_type: file_type,
                    location: Some(location.as_ref().unwrap().to_string()),
                })
            }
            _ => Err(anyhow!("Not implemented!")),
            
            }
        }

    pub fn to_relational_expression(&self, parsed_expr: &Expr, schema: &Schema) -> Result<Expression> {
        match *parsed_expr {
            Expr::Value(sqlparser::ast::Value::Boolean(b)) => Ok(Expression::Literal(ScalarValue::Boolean(b))),
            Expr::Value(sqlparser::ast::Value::Date(_)) => Err(anyhow!("Date Literals are currently not supported.")),
            Expr::Value(sqlparser::ast::Value::HexStringLiteral(_)) => Err(anyhow!("HexString Literals are currently not supported.")),
            Expr::Value(sqlparser::ast::Value::Interval{..}) => Err(anyhow!("Intervals are currently not supported.")),
            Expr::Value(sqlparser::ast::Value::NationalStringLiteral(_)) => Err(anyhow!("NationalString Literals are currently not supported.")),
            Expr::Value(sqlparser::ast::Value::Null) => Ok(Expression::Literal(ScalarValue::Null)),
            Expr::Value(sqlparser::ast::Value::Number(ref s)) => Ok(Expression::Literal(ScalarValue::Float64(s.parse::<f64>().unwrap()))),
            Expr::Value(sqlparser::ast::Value::SingleQuotedString(ref s)) => Ok(Expression::Literal(ScalarValue::Utf8(s.clone()))),
            Expr::Value(sqlparser::ast::Value::Time(_)) => Err(anyhow!("Time Literals are currently not supported.")),
            Expr::Value(sqlparser::ast::Value::Timestamp(_)) => Err(anyhow!("Timestamp Literals are currently not supported.")),

            Expr::Identifier(ref ident) => match schema.fields().iter().position(|c| c.name().eq(ident)) {
                // TODO: Handle Identifiers which are Table Names
                    Some(idx) => Ok(Expression::Column(idx)),
                    None => Err(anyhow!("Column {} not found in Schema: {} ", ident, schema.to_string())),
                },

            Expr::BinaryOp{ref left, ref op, ref right} => {
                let operator = match op {
                    BinaryOperator::Eq => Ok(Operator::Eq),
                    BinaryOperator::NotEq => Ok(Operator::NotEq),
                    BinaryOperator::Lt => Ok(Operator::Lt),
                    BinaryOperator::LtEq => Ok(Operator::LtEq),
                    BinaryOperator::Gt => Ok(Operator::Gt),
                    BinaryOperator::GtEq => Ok(Operator::GtEq),
                    BinaryOperator::And => Ok(Operator::And),
                    BinaryOperator::Or => Ok(Operator::Or),
                    BinaryOperator::NotEq => Ok(Operator::NotEq),
                    _ => Err(anyhow!("{} operator not implemented!", &op))
                };

                Ok(Expression::BinaryExpression {
                    left: Arc::new(self.to_relational_expression(left, schema)?), 
                    op: operator?, 
                    right: Arc::new(self.to_relational_expression(right, schema)?)
                })
            },

            Expr::Cast{ref expr, ref data_type} => {
                let resolved_expr = self.to_relational_expression(expr, schema)?;
                Ok(Expression::Cast{
                    expr: Arc::new(resolved_expr),
                    data_type: to_arrow_type(data_type)?,
                })
            },

            Expr::UnaryOp{ref op, ref expr} => match op {
                UnaryOperator::Not => Ok(Expression::Not(Arc::new(self.to_relational_expression(expr, schema)?))),
                other => Err(anyhow!("{:?} not implemented!", other))
            }

            Expr::IsNull(ref expr) => Ok(Expression::IsNull(Arc::new(self.to_relational_expression(expr, schema)?))),
            Expr::IsNotNull(ref expr) => Ok(Expression::IsNotNull(Arc::new(self.to_relational_expression(expr, schema)?))),

            _ => Err(anyhow!("{:?} not implemented!", parsed_expr)),
            
        }
    }

    fn build_schema(&self, columns: &Vec<ColumnDef>) -> Result<Schema> {
        let mut fields = Vec::new();
    
        for column in columns {
            let data_type = to_arrow_type(&column.data_type)?;
            let nullable = if column.options.iter().any(|e| e.option == ColumnOption::NotNull) { false } else { true };
            fields.push(Field::new(&column.name, data_type, nullable));
        }
    
        Ok(Schema::new(fields))
    }
}

/// Convert parser data types to Arrow data types
pub fn to_arrow_type(parser_dt: &sqlparser::ast::DataType) -> Result<DataType> {
    match parser_dt {
        sqlparser::ast::DataType::Boolean => Ok(DataType::Boolean),
        sqlparser::ast::DataType::SmallInt => Ok(DataType::Int16),
        sqlparser::ast::DataType::Int => Ok(DataType::Int32),
        sqlparser::ast::DataType::BigInt => Ok(DataType::Int64),
        sqlparser::ast::DataType::Float(_) | sqlparser::ast::DataType::Real => Ok(DataType::Float64),
        sqlparser::ast::DataType::Double => Ok(DataType::Float64),
        sqlparser::ast::DataType::Char(_) | sqlparser::ast::DataType::Varchar(_) => Ok(DataType::Utf8),
        sqlparser::ast::DataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        other => Err(anyhow!(
            "Unsupported parser data-type {:?}",
            other
        )),
    }
}