
#' Extracts the values of fields from a column of JSON strings, and puts them into columns named after the fields, into a destination table
#' @return A [bigrquery::bq_table()] that has each field of the JSON values spread out as individual columns.
#' @seealso [bigrquery::bq_dataset_query()], which this function wraps around.
#' @param json_fields A dataframe containing the fields of the JSON
#' @param project_name A string of the BQ project name.
#' @param dataset_name A string of the name of the dataset where table lives.
#' @param table_name A string of the name of the data table.
#' @param json_column_name A string of the name of the table column that contains the JSON values.
#' @param destination_table A table in BQ to send the results to.
#' @export
#' @examples
#' bq_get_fieldnames(json_fieldnames, "ultra-project","TEST_DATASET_3","test_table", "test_destination_table")

bq_spread_json_fields <- function (json_fields, project_name, dataset_name, table_name, json_column_name, destination_table_name=NULL) {
  generate_extraction_string <- function (fieldname) {
    destination_col_name <- stringr::str_replace_all(fieldname,"[\\[\\]]","")
    destination_col_name <- stringr::str_replace_all(destination_col_name,"[.]","_")
    keyname <- paste("['",fieldname,"']",sep="")
    paste('JSON_EXTRACT_SCALAR(json_text.',json_column_name,', "$',keyname,'") AS ',destination_col_name,sep="")
  }
  extraction_string <- paste(apply(json_fields, 1, generate_extraction_string), collapse=",")
  q <- paste('SELECT ',extraction_string,' FROM ',table_name,' AS json_text;', sep="")
  # print(q) #debug
  bq_ds <- bigrquery::bq_dataset(project_name, dataset_name)
  bq_dest_tbl <- bq_table(bq_ds, destination_table_name)
  bq_dataset_query(bq_ds, q, bq_dest_tbl)
}






