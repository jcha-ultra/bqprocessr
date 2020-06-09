#' Takes a JSON column in a BigQuery table and produces another column of the same length.
#' @return A [bigrquery::bq_table()] that has a column of the flattened JSON strings.
#' @seealso [bigrquery::bq_dataset_query()], which this function wraps around.
#' @param project_name A string of the BQ project name.
#' @param dataset_name A string of the name of the dataset where table lives.
#' @param table_name A string of the name of the data table.
#' @param json_column_name A string of the name of the table column that contains the JSON values.
#' @param destination_table A table in BQ to send the results to.
#' @export
#' @examples
#' bq_flatten_json("ultra-project","TEST_DATASET_3","test_table","string_field_0", "test_destination_table")

bq_flatten_json <- function (project_name, dataset_name, table_name, json_column_name, destination_table_name=NULL) {
  q <- paste('CREATE TEMP FUNCTION flattenedJSON (input STRING)
              RETURNS STRING
              LANGUAGE js AS """
                    var data = JSON.parse(input);
                    var result = {};
                  function recurse (cur, prop) {
                      if (Object(cur) !== cur) {
                          result[prop] = cur;
                      } else if (Array.isArray(cur)) {
                           for(var i=0, l=cur.length; i<l; i++)
                               recurse(cur[i], prop + "[" + i + "]");
                          if (l == 0)
                              result[prop] = [];
                      } else {
                          var isEmpty = true;
                          for (var p in cur) {
                              isEmpty = false;
                              recurse(cur[p], prop ? prop+"."+p : p);
                          }
                          if (isEmpty && prop)
                              result[prop] = {};
                      }
                  }
                  recurse(data, "");
                  return JSON.stringify(result);
              """;
              SELECT
                flattenedJSON(',json_column_name,') AS flattened_json
              FROM
                ',table_name,'
              WHERE ',json_column_name,' IS NOT NULL', 
            sep="")
  bq_ds <- bigrquery::bq_dataset(project_name, dataset_name)
  bq_dest_tbl <- bq_table(bq_ds, destination_table_name)
  bq_dataset_query(bq_ds, q, bq_dest_tbl)
}

