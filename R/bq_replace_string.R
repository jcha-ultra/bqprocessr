
#' Replaces a specified substring in the table, across specified columns
#' @return A [bigrquery::bq_table()] that has each field of the JSON values spread out as individual columns.
#' @seealso [bigrquery::bq_dataset_query()], which this function wraps around.
#' @param column_names Vector of column names to replace the strings in.
#' @param str_to_replace String that will be replaced.
#' @param column_names String that will be the replacement.
#' @param project_name A string of the BQ project name.
#' @param dataset_name A string of the name of the dataset where table lives.
#' @param table_name A string of the name of the data table.
#' @param destination_table A table in BQ to send the results to.
#' @export
#' @examples
#' bq_get_fieldnames(json_fieldnames, "ultra-project","TEST_DATASET_3","test_table", "test_destination_table")

bq_replace_string <- function (column_names, str_to_replace, new_string, project_name, dataset_name, table_name, destination_table_name=NULL) {
  generate_replace_statement <- function (col_name) {
    paste('REPLACE (',col_name,', "',str_to_replace,'", "',new_string,'") as ',col_name, sep="")
  }
  replacement_string <- paste(lapply(as.list(column_names), generate_replace_statement), collapse=",")
  q <- paste('SELECT ',replacement_string,' FROM ',table_name,sep="")
  bq_ds <- bigrquery::bq_dataset(project_name, dataset_name)
  
  if(!is.null(destination_table_name)) {
    bq_dest_tbl <- bq_table(bq_ds, destination_table_name)
    result <- bq_dataset_query(bq_ds, q, bq_dest_tbl)
  } else {
    result <- bq_dataset_query(bq_ds, q)
  }
  return(result)
}





