#' Gets a list of first-level field names of all JSON values in a BQ column
#'
#' @return A dataframe listing out the first-level field names of the JSONs in the specified column; this is the union of all keys used in any JSON there.
#' @seealso [bigrquery::bq_dataset_query()], which this function wraps around.
#' @param project_name A string of the BQ project name
#' @param dataset_name A string of the name of the dataset where table lives
#' @param table_name A string of the name of the data table
#' @param json_column_name A string of the name of the table column that contains the JSON values
#' @export
#' @examples
#' bq_get_json_fieldnames("ultra-project","TEST_DATASET_3","test_demo_flattened","flattened_json")
bq_get_json_fieldnames <- function (project_name, dataset_name, table_name, json_column_name) {
  q <- paste('CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
            RETURNS Array<String>
            LANGUAGE js AS """
              return Object.keys(JSON.parse(input));
            """;
            WITH keys AS (
              SELECT
                jsonObjectKeys(',json_column_name,') AS keys
              FROM
                ',table_name,'
              WHERE ',json_column_name,' IS NOT NULL
            )
            SELECT
              DISTINCT field_names
            FROM keys
            CROSS JOIN UNNEST(keys.keys) AS field_names
            ORDER BY field_names', sep="")
  bigrquery::bq_table_download(bq_dataset_query(bigrquery::bq_dataset(project_name, dataset_name), q))
}