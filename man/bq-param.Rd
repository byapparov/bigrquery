% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bq-param.R
\name{bq-param}
\alias{bq-param}
\alias{bq_param_scalar}
\alias{bq_param_array}
\title{Defines parameter that can be passed to the query}
\usage{
bq_param_scalar(value, type = NULL, name = NULL)

bq_param_array(value, type = NULL, name = NULL)
}
\arguments{
\item{value}{vector of parameter values}

\item{type}{BigQuery type of the parameter}

\item{name}{name of the parameter in the query, omitting the \code{@}}
}
\description{
Defines parameter that can be passed to the query
}
