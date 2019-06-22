#' BigQuery tables
#'
#' Basic create-read-update-delete verbs for tables, as well as functions
#' for uploading and downloading data in to/from memory (`bq_table_upload()`,
#' (`bq_table_download()`)), and saving to/loading from Google CloudStorage
#' (`bq_table_load()`, `bq_table_save()`).
#'
#' @param x A [bq_table], or an object coercible to a `bq_table`.
#' @inheritParams api-job
#' @inheritParams api-perform
#' @inheritParams bq_projects
#' @section API documentation:
#' * [insert](https://developers.google.com/bigquery/docs/reference/v2/tables/insert)
#' * [get](https://developers.google.com/bigquery/docs/reference/v2/tables/get)
#' * [delete](https://developers.google.com/bigquery/docs/reference/v2/tables/delete)
#' @return
#' * `bq_table_copy()`, `bq_table_create()`, `bq_table_delete()`, `bq_table_upload()`:
#'   an invisible [bq_table]
#' * `bq_table_exists()`: either `TRUE` or `FALSE`.
#' * `bq_table_download()`: a data frame
#' * `bq_table_size()`: the size of the table in bytes
#' * `bq_table_fields()`: a [bq_fields].
#'
#' @examples
#' if (bq_testable()) {
#' ds <- bq_test_dataset()
#'
#' bq_mtcars <- bq_table(
#'   ds,
#'   "mtcars",
#'   friendly_name = "Motor Trend Car Road Tests",
#'   description = "The data was extracted from the 1974 Motor Trend US magazine",
#'   labels = list(category = "example")
#' )
#' bq_table_exists(bq_mtcars)
#'
#' bq_table_upload(bq_mtcars, mtcars)
#' bq_table_exists(bq_mtcars)
#'
#' bq_table_fields(bq_mtcars)
#' bq_table_size(bq_mtcars)
#' str(bq_table_meta(bq_mtcars))
#'
#' bq_table_delete(bq_mtcars)
#' bq_table_exists(bq_mtcars)
#'
#' my_natality <- bq_table(ds, "mynatality")
#' bq_table_copy("publicdata.samples.natality", my_natality)
#' }
#' @name api-table
NULL

#' @export
#' @rdname api-table
#' @param fields A [bq_fields] specification, or something coercible to it
#'   (like a data frame).
bq_table_create <- function(x, fields = NULL, ...) {
  x <- as_bq_table(x)

  url <- bq_path(x$project, x$dataset, "")
  body <- list(
    tableReference = tableReference(x)
  )
  if (!is.null(fields)) {
    fields <- as_bq_fields(fields)
    body$schema <- list(fields = as_json(fields))
  }

  bq_post(url, body = bq_body(body, ...))

  x
}

#' @export
#' @rdname api-table
#' @inheritParams api-job
bq_table_meta <- function(x, fields = NULL) {
  x <- as_bq_table(x)
  url <- bq_path(x$project, x$dataset, x$table)
  bq_get(url, query = list(fields = fields))
}

#' @export
#' @rdname api-table
bq_table_fields <- function(x) {
  meta <- bq_table_meta(x, fields = "schema")
  fields <- meta$schema$fields

  bq_fields(lapply(fields, as_bq_field))
}

#' @export
#' @rdname api-table
bq_table_size <- function(x) {
  meta <- bq_table_meta(x, fields = "numBytes")
  bytes <- as.numeric(meta$numBytes)
  structure(bytes, class = "bq_bytes")
}

#' @export
#' @rdname api-table
bq_table_nrow <- function(x) {
  meta <- bq_table_meta(x, fields = "numRows")
  as.numeric(meta$numRows)
}

#' @export
#' @rdname api-table
bq_table_exists <- function(x) {
  x <- as_bq_table(x)
  url <- bq_path(x$project, x$dataset, x$table)
  bq_exists(url)
}

#' @export
#' @rdname api-table
bq_table_delete <- function(x) {
  x <- as_bq_table(x)
  url <- bq_path(x$project, x$dataset, x$table)
  invisible(bq_delete(url))
}

#' @export
#' @rdname api-table
#' @inheritParams bq_perform_copy
#' @param dest Source and destination [bq_table]s.
bq_table_copy <- function(x, dest, ..., quiet = NA) {
  x <- as_bq_table(x)
  dest <- as_bq_table(dest)

  job <- bq_perform_copy(x, dest, ...)
  bq_job_wait(job, quiet = quiet)

  dest
}

#' @export
#' @rdname api-table
#' @inheritParams api-perform
bq_table_upload <- function(x, values, ..., quiet = NA) {
  x <- as_bq_table(x)

  job <- bq_perform_upload(x, values, ...)
  bq_job_wait(job, quiet = quiet)

  invisible(x)
}

#' @export
#' @rdname api-table
bq_table_save <- function(x, destination_uris, ..., quiet = NA) {
  x <- as_bq_table(x)

  job <- bq_perform_extract(x, destination_uris = destination_uris, ...)
  bq_job_wait(job, quiet = quiet)

  invisible(x)
}

#' @export
#' @rdname api-table
bq_table_load <- function(x, source_uris, ..., quiet = NA) {
  x <- as_bq_table(x)

  job <- bq_perform_load(x, source_uris = source_uris, ...)
  bq_job_wait(job, quiet = quiet)

  invisible(x)
}

#' @export
#' @rdname api-table
bq_table_patch <- function(x, fields) {
  x <- as_bq_table(x)

  url <- bq_path(x$project, x$dataset, x$table)
  body <- list(
    tableReference = tableReference(x)
  )
  fields <- as_bq_fields(fields)
  body$schema <- list(fields = as_json(fields))
  bq_patch(url, body)
}

#' Streams data from data.from into BigQUery
#'
#' @param x bq_table where values will be streamed to
#' @param values data.frame that will be streamed
#' @param skip_invalid_rows
#'   (optional) Insert all valid rows of a request,
#'   even if invalid rows exist. The default value is False, which
#'   causes the entire request to fail if any invalid rows exist.
#' @param ignore_unknown_values
#'   (optional) Accept rows that contain values that do not match the
#'   schema. The unknown values are ignored. Default is False, which
#'   treats unknown values as errors.
#' @param template_suffix
#'   (optional) treat `name` as a template table and provide a suffix.
#'   BigQuery will create the table `<name> + <template_suffix>` based
#'   on the schema of the template table. See
#'   https://cloud.google.com/bigquery/streaming-data-into-bigquery#template-tables
#' @export
#' @rdname bq-streaming
#' @seealso https://cloud.google.com/bigquery/streaming-data-into-bigquery
bq_table_stream <- function(x, values, skip_invalid_rows = "false", ignore_unknown_values = "false", template_suffix = NULL) {
  x <- as_bq_table(x)
  url <- bq_path(x$project, x$dataset, x$table)
  url <- paste0(url, "/", "insertAll")
  body <- bq_stream_body(
    x = values,
    skip_invalid_rows = skip_invalid_rows,
    ignore_unknown_values = ignore_unknown_values,
    template_suffix = template_suffix
  )
  print(jsonlite::toJSON(body, pretty = TRUE, auto_unbox = TRUE))
  bq_post(url, body)
}

#' Converts data.table to streaming request body
#'
#' @noRd
#' @return list that represents json for streaming
#' @seealso https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll#InsertionRow
bq_stream_body <- function(x, skip_invalid_rows = "false", ignore_unknown_values = "false", template_suffix = NULL) {
  l <- setNames(split(x, seq(nrow(x))), rownames(x))
  rows <- lapply(l, function(row) {
    list(
      insertId = paste(sample(c(letters[1:6], 0:9), 30, replace = TRUE), collapse = ""),
      json = as.list(row)
    )
  })
  names(rows) <- NULL
  body <- list(
    kind = "bigquery#tableDataInsertAllRequest",
    skipInvalidRows = skip_invalid_rows,
    ignoreUnknownValues = ignore_unknown_values,
    rows = rows
  )

  if (!is.null(template_suffix)) {
    body["templateSuffix"] <- template_suffix
  }
  body
}
