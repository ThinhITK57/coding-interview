from spark_linage_visitor import *

file_name = r"C:\Users\namtv40\Projects\bi-datalake-infra\spark-jobs\executor\jobs\bi_silver\crm_deals.py"
with open(file_name, 'r', encoding="utf-8") as f:
    tree = ast.parse(f.read())

    visitor = SparkLineageVisitor()
    visitor.visit(tree)

    for edge in visitor.get_lineage():
        print(edge)