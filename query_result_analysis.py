from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf,lit,to_json,col
from snowflake.snowpark.types import StringType,IntegerType,FloatType,BooleanType,StructType,StructField


import os
import re
import json
import warnings
import pandas as pd

from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import build_scope
from thefuzz import fuzz


# Fill - Snowflake connection parameters
connection_parameters = {
   "account": "",
   "user": "",
   "role":"",
   "Password":"",
   "database": "",
   "schema": "",
   "warehouse":""
}


warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    module='snowflake.connector'
)

# Fill - Snowflake Query Id, Snowflake Query file path

query_id_code = [("01bb4c8c-3204-3c79-0002-2fc60004502e", "C:/Users/Narendra Meenaga/OneDrive - Acumen Consulting Ltd/Desktop/Code/Snowflake-Performance-Analysis/Examples/sample_query_1.sql"),
                 ("01bb8f79-3204-4832-0002-2fc6000c92fa", "C:/Users/Narendra Meenaga/OneDrive - Acumen Consulting Ltd/Desktop/Code/Snowflake-Performance-Analysis/Examples/sample_query_2.sql"),
                 ("01bb89b2-3204-463f-0002-2fc6000b5696", "C:/Users/Narendra Meenaga/OneDrive - Acumen Consulting Ltd/Desktop/Code/Snowflake-Performance-Analysis/Examples/sample_query_3.sql"), 
                 ("01bb6e5f-3204-43bd-0002-2fc60007102a", "C:/Users/Narendra Meenaga/OneDrive - Acumen Consulting Ltd/Desktop/Code/Snowflake-Performance-Analysis/Examples/sample_query_4.sql"),
                 ("01bb8fea-3204-48f5-0002-2fc6000cd012", "C:/Users/Narendra Meenaga/OneDrive - Acumen Consulting Ltd/Desktop/Code/Snowflake-Performance-Analysis/Examples/sample_query_5.sql")]


# Fill - Results Excel sheet path
results_file_path = ".../results.xlsx"

# Values to set
terms_to_remove =  "WHERE\s|\sIN\s|\sAND\s|\sOR\s|\sCAST|\sAS\s|\(|\)"
string_compare_threshold = 50
output_to_input_rows_ratio_threshold   = 1
bytes_spilled_local_storage_threshold  = 0
bytes_spilled_remote_storage_threshold = 0
partitions_scanned_ratio_threshold     = 0.9
queued_overload_time_threshold         = 0


# Create Session
session = Session.builder.configs(connection_parameters).create()  

parameters_schema = StructType([
    StructField("Query ID", StringType()),
    StructField("Warehouse Size", StringType()),
    StructField("Bytes Scanned", StringType())
])

temp_result_schema = StructType([
    StructField("query_id", StringType()),
    StructField("rule_number", IntegerType()),
    StructField("rule_type", StringType()),
    StructField("rule_description", StringType()),
    StructField("result", StringType()),
    StructField("comments", StringType()),
    StructField("operator_id", StringType())
])

final_result_schema = StructType([
    StructField("query_id", StringType()),
    StructField("rule_number", IntegerType()),
    StructField("rule_type", StringType()),
    StructField("rule_description", StringType()),
    StructField("result", StringType()),
    StructField("comments", StringType()),
    StructField("corresponding_code", StringType())
])


params_df  = session.create_dataframe([], parameters_schema)
result_df  = session.create_dataframe([], final_result_schema)

expr_alias_dict = {}



def query_operator_stats(query_id):

    stats_df = session.sql("select * from table(get_query_operator_stats('"  + query_id + "'))")
    
    result_stats_df = stats_df.withColumn('execution_time_percentage_num', stats_df['execution_time_breakdown']['overall_percentage'])\
                              .withColumn('bytes_spilled_local_storage', stats_df['operator_statistics']['spilling']['bytes_spilled_local_storage'])\
                              .withColumn('bytes_spilled_remote_storage', stats_df['operator_statistics']['spilling']['bytes_spilled_remote_storage'])\
                              .withColumn('partitions_scanned_ratio', stats_df['operator_statistics']['pruning']['partitions_scanned']/stats_df['operator_statistics']['pruning']['partitions_total'])\
                              .select('query_id', 'operator_id', 'operator_type', 'operator_attributes', 'operator_statistics', 'bytes_spilled_local_storage', 'bytes_spilled_remote_storage', 'partitions_scanned_ratio', 'execution_time_percentage_num')

    return result_stats_df


def query_history_stats(query_id):
    history_df = session.sql("select * from table(information_schema.query_history(result_limit=>10000)) where query_id = '" + query_id + "'")

    result_history_df = history_df.filter(col('total_elapsed_time') > 0)\
                              .filter(col('error_code').is_null())\
                              .filter(col('bytes_scanned').is_not_null())\
                              .select('query_id', 'warehouse_size', 'bytes_scanned', 'total_elapsed_time', 'rows_produced', 'compilation_time', 'execution_time', 'queued_overload_time')
    return result_history_df

# Remove all comments 
# 1. multiline  /*   */
# 2. single line --
def remove_comments(input_txt):
    comments_pattern = '(\/\*((.|\n)*?)\*\/)|\-\-(.*)\n'
    result_txt = re.sub(comments_pattern, '', input_txt)
    return(result_txt.strip())


def parse_sql(sql_file):
    sql_main_code = {}

    if os.path.exists(sql_file):
        with open(sql_file) as file:
        
            sql_code = file.read()
        
            # Remove all comments
            sql_code = remove_comments(sql_code)

            ast = parse_one(sql_code, dialect="snowflake")

            print(ast.args.keys())

            for select in ast.find_all(exp.Select):
                for expr in select.expressions:       
                    if expr.alias:
                        column_expr = str(expr).lower().replace(expr.alias, '').replace('as', '')
                        expr_alias_dict[column_expr.strip()] = expr.alias.strip()

   
            if 'from' in ast.args:
                sql_main_code['from_line'] = str(ast.args['from'])
                sql_main_code['from_table'] = re.search("from\s+((\w|\.)+)", sql_main_code['from_line'], re.IGNORECASE).group(1)
                
            if 'where' in ast.args:
                sql_main_code['where'] = str(ast.args['where'])
                        
            if 'joins' in ast.args:
                sql_main_code['joins'] = ast.args['joins']
                
            if 'order' in ast.args:
                sql_main_code['order'] = str(ast.args['order'])

            if 'group' in ast.args:
                sql_main_code['group'] = str(ast.args['group'])

            if 'expressions' in ast.args:
                select_expression = 'SELECT '
                for expr in ast.args['expressions']:
                    select_expression += str(expr) + ', '
                sql_main_code['select'] = select_expression[:-2]

            if 'with' in ast.args:
                cte_list = []
                for expr in ast.args['with']:
                  cte_list.append(str(expr))
                  sql_main_code['cte'] = cte_list

                   
    else :
        print("File doesnot exists")
        exit()

    return sql_main_code

def parse_cte_sql(sql_cte_code_list):
    sql_cte_code_dict_dict = {}
    for sql_cte_code in sql_cte_code_list:
 
        sql_cte_code_dict = {}

        cte_name = re.search("((\w|_)+)\s+AS\s+\(((\n|.)*)\)", sql_cte_code, re.IGNORECASE).group(1)
        cte_code = re.search("((\w|_)+)\s+AS\s+\(((\n|.)*)\)", sql_cte_code, re.IGNORECASE).group(3)

        ast = parse_one(cte_code, dialect="snowflake")

        if 'from' in ast.args:
                sql_cte_code_dict['from_line'] = str(ast.args['from'])
                sql_cte_code_dict['from_table'] = re.search("from\s+((\w|\.)+)", sql_cte_code_dict['from_line'], re.IGNORECASE).group(1)

        if 'where' in ast.args:
            sql_cte_code_dict['where'] = str(ast.args['where'])
                    
        if 'joins' in ast.args:
            sql_cte_code_dict['joins'] = ast.args['joins']
            
        if 'order' in ast.args:
            sql_cte_code_dict['order'] = str(ast.args['order'])

        if 'group' in ast.args:
            sql_cte_code_dict['group'] = str(ast.args['group'])

        if 'expressions' in ast.args:
            select_expression = 'SELECT '
            for expr in ast.args['expressions']:
                select_expression += str(expr) + ', '
                if 'over' in str(expr).lower():
                    sql_cte_code_dict['window'] = str(expr)

            sql_cte_code_dict['select'] = select_expression[:-2]

        sql_cte_code_dict_dict[cte_name] = sql_cte_code_dict
 
    return sql_cte_code_dict_dict

    

    
def identify_code(stats_df, sql_main_code, sql_cte_code):

    operator_code_schema = StructType([
                            StructField("operator_id", IntegerType()),
                            StructField("operator_type", StringType()),
                            StructField("corresponding_code", StringType())
                            ])

    operator_code_df = session.create_dataframe([], operator_code_schema)
    stats_sorted_df = stats_df.sort("OPERATOR_ID", ascending=False)

    table_scan_flag = False
    filter_flag = False
    join_flag = False 
    sort_flag = False
    result_flag = False
    cartesionjoin_flag = False 
    aggregate_flag = False
    windowfunction_flag = False


    for row in stats_sorted_df.to_local_iterator():
        operator_attributes_json = json.loads(row['OPERATOR_ATTRIBUTES'])

        code_line = ''

        match row['OPERATOR_TYPE']:
            case 'TableScan':
                if 'table_alias' in operator_attributes_json:
                    if sql_main_code['from_table'].lower() == operator_attributes_json['table_name'].lower():
                        table_scan_line = ' '.join([val for val in ['from',operator_attributes_json['table_name'], operator_attributes_json['table_alias']]] )
                        table_scan_flag = True
                    else:
                        table_scan_line =  ' '.join([val for val in [operator_attributes_json['table_name'], operator_attributes_json['table_alias']]] )
                else:
                    if sql_main_code['from_table'].lower() == operator_attributes_json['table_name'].lower():
                        table_scan_line = ' '.join([val for val in ['from',operator_attributes_json['table_name']]] )
                        table_scan_flag = True
                    else:
                        table_scan_line =  ' '.join([val for val in [operator_attributes_json['table_name']]] )
                code_line = str(table_scan_line)
                

            case 'Filter':
                if 'where' in sql_main_code:
                    where_line = re.sub(terms_to_remove, " ", sql_main_code['where'], re.MULTILINE)
                    operator_filter = re.sub(terms_to_remove, " ", operator_attributes_json['filter_condition'])
                    if(fuzz.ratio(where_line.lower(),   operator_filter.lower())) > string_compare_threshold:
                            code_line = operator_attributes_json['filter_condition']
                            filter_flag = True
                    # Check Alias - To make it iterative to replace all alias with the corresponding column expressions
                    else :
                        for expr, alias in expr_alias_dict.items():
                            if alias in where_line:
                                where_line_replaced = where_line.replace(alias, expr).lower()
                                if(fuzz.ratio(where_line_replaced,   operator_filter.lower())) > string_compare_threshold:
                                    code_line = where_line_replaced       
                                    filter_flag = True


            case 'Join':
                join_conditions_list = re.findall("\((\s*(\w|\.)+\s*=\s*(\w|\.)+\s*)\)", operator_attributes_json['equality_join_condition'], re.IGNORECASE)
                for join_condition in join_conditions_list: 
                    for join_table_line in sql_main_code['joins']:
                        if(fuzz.ratio(join_condition[0].lower(), str(join_table_line).lower())) > string_compare_threshold:
                            code_line = str(join_table_line)
                            join_flag = True

            case 'Sort':
                if 'order' in sql_main_code:
                    for sort_key_str in operator_attributes_json['sort_keys']:
                        #sort_key = re.match("(\w|\.)+", sort_key_str, re.IGNORECASE).group(0)
                        sort_key = re.match("(\w+\.)*(\w+)", sort_key_str, re.IGNORECASE).group(2)
                        if sort_key.lower() in sql_main_code['order'].lower():
                            sort_flag = True
                    if sort_flag:
                        code_line = sql_main_code['order']
                        sort_flag = True

            case 'Result':
                if str(sql_main_code['select']).strip().lower() != 'select *':
                    for column_name in operator_attributes_json['expressions']:
                        if column_name.lower() in sql_main_code['select'].lower():
                            result_flag = True
                else:
                    result_flag = True
                if result_flag:
                    code_line = str(sql_main_code['select'])
                    result_flag = True

            case 'CartesianJoin':
                code_line = ' '.join([val for val in [sql_main_code['from_line'] , str(sql_main_code['joins'][0])]])
                cartesionjoin_flag = True

            case 'Aggregate':
                if 'group' in  sql_main_code:
                    code_line = ' '.join([val for val in [str(sql_main_code['select']), '\n...\n', sql_main_code['group']]])
                    aggregate_flag = True
      
            case 'WindowFunction':
                if 'window' in  sql_main_code:
                    code_line = sql_main_code['window']
                    windowfunction_flag = False

            case 'JoinFilter':
                continue

            case 'QUERY RESULT REUSE':
                print("Query Result Reuse")

            case _:
                print("Undefined Operator Type")

        if code_line != '':
            new_row    = [(str(row['OPERATOR_ID']), row['OPERATOR_TYPE'], code_line)]
            new_row_df = session.create_dataframe(new_row, operator_code_schema)
            operator_code_df  = operator_code_df.union(new_row_df)
    
        elif parse_cte_sql != {}: 
            match row['OPERATOR_TYPE']:
                case 'TableScan':
                    if not table_scan_flag:
                        if 'table_alias' in operator_attributes_json :
                                for cte_name in sql_cte_code.keys():
                                    for key, value in sql_cte_code[cte_name].items():
                                        if key == 'from_table' and  value.lower() == operator_attributes_json['table_name'].lower():
                                            table_scan_line = ' '.join([val for val in ['from',operator_attributes_json['table_name'], operator_attributes_json['table_alias']]] )
                                            table_scan_flag = True
                        else:
                            if 'table_alias' in operator_attributes_json :
                                for cte_name in sql_cte_code.keys():
                                    for key, value in sql_cte_code[cte_name].items():
                                        if key == 'from_table' and  value.lower() == operator_attributes_json['table_name'].lower():
                                            table_scan_line = ' '.join([val for val in ['from',operator_attributes_json['table_name']]] )
                                            table_scan_flag = True

                        code_line = str(table_scan_line)
                    

                case 'Filter':
                    if not filter_flag:
                        for cte_name in sql_cte_code.keys():
                                for key, value in sql_cte_code[cte_name].items():
                                    if 'where' in sql_cte_code[cte_name]:
                                        where_line = re.sub(terms_to_remove, " ", sql_cte_code[cte_name]['where'])
                                        operator_filter = re.sub(terms_to_remove, " ", operator_attributes_json['filter_condition']) 
                                        if(fuzz.ratio(where_line.lower(),   operator_filter.lower())) > string_compare_threshold:
                                                code_line = operator_attributes_json['filter_condition']
                                                filter_flag = True
                                        # Check Alias - To make it iterative to replace all alias with the corresponding column expressions
                                        else :
                                            for expr, alias in expr_alias_dict.items():
                                                if alias in where_line:
                                                    where_line_replaced = where_line.replace(alias, expr).lower()
                                                    if(fuzz.ratio(where_line_replaced,   operator_filter.lower())) > string_compare_threshold:
                                                        code_line = where_line_replaced       
                                                        filter_flag = True                                

                case 'Join':
                    if not join_flag:
                        for cte_name in sql_cte_code.keys():
                                for key, value in sql_cte_code[cte_name].items():
                                    if 'joins' in sql_cte_code[cte_name]:
                                        join_conditions_list = re.findall("\((\s*(\w|\.)+\s*=\s*(\w|\.)+\s*)\)", operator_attributes_json['equality_join_condition'], re.IGNORECASE)
                                        for join_condition in join_conditions_list: 
                                            for join_table_line in sql_cte_code[cte_name]['joins']:
                                                if (join_condition[0].lower() in str(join_table_line).lower()):
                                                    code_line = str(join_table_line)
                                                    join_flag = True

                case 'Sort':
                    if not sort_flag:
                        for cte_name in sql_cte_code.keys():
                                for key, value in sql_cte_code[cte_name].items():
                                    if 'order' in sql_cte_code[cte_name]:
                                        for sort_key_str in operator_attributes_json['sort_keys']:
                                            sort_key = re.match("(\w|\.)+", sort_key_str, re.IGNORECASE).group(0)
                                            if sort_key.lower()  in sql_cte_code[cte_name]['order'].lower():
                                                sort_flag = True
                                        if sort_flag:
                                            code_line = sql_cte_code[cte_name]['order']
                                            sort_flag = True

                case 'Result':
                    if not result_flag:
                        for cte_name in sql_cte_code.keys():
                                for key, value in sql_cte_code[cte_name].items():
                                    if 'select' in sql_cte_code[cte_name]:
                                        if str(sql_cte_code[cte_name]['select']).strip().lower() != 'select *':
                                            for column_name in operator_attributes_json['expressions']:
                                                if column_name.lower() not in sql_cte_code[cte_name]['select'].lower():
                                                    result_flag = True
                                        if result_flag:
                                            code_line = str(sql_cte_code[cte_name]['select'])
                                            result_flag = True

                case 'CartesianJoin':
                    if not cartesionjoin_flag:
                        code_line = ' '.join([val for val in [sql_cte_code[cte_name]['from_line'] , str(sql_cte_code[cte_name]['joins'][0])]])
                        cartesionjoin_flag = True

                case 'Aggregate':
                    if not aggregate_flag:
                        for cte_name in sql_cte_code.keys():
                                for key, value in sql_cte_code[cte_name].items():
                                    if 'group' in  sql_cte_code[cte_name]:
                                        code_line = ' '.join([val for val in [str(sql_cte_code[cte_name]['select']), '\n...\n', sql_cte_code[cte_name]['group']]])
                                        aggregate_flag = True

                case 'WindowFunction':
                    if not windowfunction_flag:
                            for cte_name in sql_cte_code.keys():
                                for key, value in sql_cte_code[cte_name].items():
                                    if 'window' in  sql_cte_code[cte_name]:
                                        code_line = sql_cte_code[cte_name]['window']
                                        windowfunction_flag = True
            
                case 'JoinFilter':
                    continue

                case 'QUERY RESULT REUSE':
                    print("Query Result Reuse")

                case _:
                    print("Undefined Operator Type")



            new_row    = [(str(row['OPERATOR_ID']), row['OPERATOR_TYPE'], code_line)]
            new_row_df = session.create_dataframe(new_row, operator_code_schema)
            operator_code_df  = operator_code_df.union(new_row_df)
 
 
    return operator_code_df




for tuple_item in query_id_code:
    query_id,  sql_file = tuple_item
    
    print("\nAnalysing Query ID : " + query_id)
    print("---------------------------------------------------------")

    stats_df  = query_operator_stats(query_id)  

    sql_main_code     = parse_sql(sql_file)
    if 'cte' in sql_main_code:
        sql_cte_code  = parse_cte_sql(sql_main_code['cte'])
    else :
        sql_cte_code = {}

    operator_code_df   = identify_code(stats_df, sql_main_code, sql_cte_code)
    operator_code_df.sort('operator_id', ascending=False).show(50)


    history_df = query_history_stats(query_id)

    warehouse_size  = history_df.select('warehouse_size').collect()[0][0]
    bytes_scanned   = history_df.select('bytes_scanned').collect()[0][0]
    
    new_row    = [(query_id, str(warehouse_size), str(bytes_scanned))]
    new_row_df = session.create_dataframe(new_row, parameters_schema)
    params_df  = params_df.union(new_row_df)

    stats_history_df = stats_df.join(history_df, 'query_id')
    stats_history_df = stats_history_df.withColumn('execution_time_secs', (col('execution_time_percentage_num') * col('execution_time')) / 1000)

 
    # Rule 1 - Identify exploding joins - Each input row gives more than one output row
    rule_type = 'Query'
    rule_df = stats_history_df.filter(col('operator_type').in_('Join', 'CartesianJoin'))\
                            .withColumn('output_to_input_rows_ratio', stats_history_df['operator_statistics']['output_rows']/stats_history_df['operator_statistics']['input_rows'])\
                            .filter(col('output_to_input_rows_ratio') > output_to_input_rows_ratio_threshold)
                

    if(rule_df.count() >= 1):
        result = 'Fail'
        comments = rule_df.select('output_to_input_rows_ratio').collect()[0][0]
        operator_id = str(rule_df.select('operator_id').collect()[0][0])
    else:
        result = 'Pass'
        comments =  ''
        operator_id = ''

    print("Rule 1 - Identify exploding joins : " + result)


    new_row    = [(query_id, 1, rule_type, 'Exploding Joins', result, str(comments), operator_id)]
    new_row_df = session.create_dataframe(new_row, temp_result_schema)

    if operator_id == '' :
        result_df  = result_df.union(new_row_df)
    else:
        result_df  = result_df.union(new_row_df.join(operator_code_df, 'operator_id').select('query_id', 'rule_number', 'rule_type', 'rule_description', 'result', 'comments', 'corresponding_code'))


    # Rule 2 - Check for Memory Spillage to Local Storage - 'bytes_spilled_local_storage' in 'operator_statistics' should be > 0
    rule_type = 'Query, Warehouse'
    rule_df = stats_history_df.filter(col('bytes_spilled_local_storage') > bytes_spilled_local_storage_threshold)

    if(rule_df.count() >= 1):
        result = 'Fail'
        comments = rule_df.select('bytes_spilled_local_storage').collect()[0][0]
        operator_id = str(rule_df.select('operator_id').collect()[0][0])
    else:
        result = 'Pass'
        comments = ''
        operator_id = ''
   

    print("Rule 2 - Memory Spillage-Local Storage : " +  result)

    new_row    = [(query_id, 2, rule_type, 'Memory Spillage-Local Storage', result, str(comments), operator_id)]
    new_row_df = session.create_dataframe(new_row, temp_result_schema)

    if operator_id == '' :
        result_df  = result_df.union(new_row_df)
    else:
        result_df  = result_df.union(new_row_df.join(operator_code_df, 'operator_id').select('query_id', 'rule_number', 'rule_type', 'rule_description', 'result', 'comments', 'corresponding_code'))

   # Rule 3 - Check for Memory Spillage to Remote Storage - 'bytes_spilled_remote_storage' in 'operator_statistics' shouls be > 0
    rule_type = 'Query, Warehouse'
    rule_df = stats_history_df.filter(col('bytes_spilled_remote_storage') > bytes_spilled_remote_storage_threshold)

    if(rule_df.count() >= 1):
        result = 'Fail'
        comments = rule_df.select('bytes_spilled_remote_storage').collect()[0][0]
        operator_id = str(rule_df.select('operator_id').collect()[0][0])
    else:
        result = 'Pass'
        comments = ''
        operator_id = ''
   

    print("Rule 3 - Memory Spillage-Remote Storage : " +  result)

    new_row    = [(query_id, 3, rule_type, 'Memory Spillage-Remote Storage', result, str(comments), operator_id)]
    new_row_df = session.create_dataframe(new_row, temp_result_schema)
   
    if operator_id == '' :
        result_df  = result_df.union(new_row_df)
    else:
        result_df  = result_df.union(new_row_df.join(operator_code_df, 'operator_id').select('query_id', 'rule_number', 'rule_type', 'rule_description', 'result', 'comments', 'corresponding_code'))
   

    # Rule 4 - Inefficient partition pruning - more than 90% of the partitions are scanned
    rule_type = 'Query, Warehouse'
    rule_df = stats_history_df.filter(col('partitions_scanned_ratio') >= partitions_scanned_ratio_threshold)

    if(rule_df.count() >= 1):
        result = 'Fail'
        comments = rule_df.select('partitions_scanned_ratio').collect()[0][0]
        operator_id = str(rule_df.select('operator_id').collect()[0][0])
    else:
        result = 'Pass'
        comments = ''
        operator_id = ''
   

    print("Rule 4 - Inefficient partition pruning : " +  result)
    
    new_row    = [(query_id, 4, rule_type, 'Inefficient partition pruning', result, str(comments),  operator_id)]
    new_row_df = session.create_dataframe(new_row, temp_result_schema)

    if operator_id == '' :
        result_df  = result_df.union(new_row_df)
    else:
        result_df  = result_df.union(new_row_df.join(operator_code_df, 'operator_id').select('query_id', 'rule_number', 'rule_type', 'rule_description', 'result', 'comments', 'corresponding_code'))
   
    # Rule 5 - Query Queuing - check if queries are stuck in the queue
    rule_type = 'Warehouse'
    rule_df = stats_history_df.filter(col('queued_overload_time') > queued_overload_time_threshold)

    if(rule_df.count() >= 1):
        result = 'Fail'
        comments = rule_df.select('queued_overload_time').collect()[0][0]
        operator_id = str(rule_df.select('operator_id').collect()[0][0])
    else:
        result = 'Pass'
        comments = ''
        operator_id = ''
   

    print("Rule 5 - Query Queuing : " +  result)

    new_row    = [(query_id, 5, rule_type, 'Query Queuing', result, str(comments), operator_id)]
    new_row_df = session.create_dataframe(new_row, temp_result_schema)

    if operator_id == '' :
        result_df  = result_df.union(new_row_df)
    else:
        result_df  = result_df.union(new_row_df.join(operator_code_df, 'operator_id').select('query_id', 'rule_number', 'rule_type', 'rule_description', 'result', 'comments', 'corresponding_code'))
   

   # Rule 6 - Check 'Union without All'
    rule_type = 'Query'
    rule_df = stats_history_df.filter(col('queued_overload_time') > queued_overload_time_threshold)




print("\nSnowflake Environment : ")
params_df.show()

print("\nResults of Query Analysis : ")
result_df.sort( col('query_id'), col('rule_number')).show(50)

with pd.ExcelWriter(results_file_path) as writer:
    params_df.to_pandas().to_excel(writer, sheet_name="Environment", header=True, index=False)
    result_df.to_pandas().to_excel(writer, sheet_name="Results", header=True, index=False)
    
print("Results are saved to : " + str(results_file_path)) 


# Close Session
session.close