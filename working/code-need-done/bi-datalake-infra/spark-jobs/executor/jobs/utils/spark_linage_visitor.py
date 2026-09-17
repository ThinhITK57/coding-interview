import ast
from typing import Dict, Set, List

import sqlglot
from sqlglot import exp


class SparkLineageVisitor(ast.NodeVisitor):

    def __init__(self):

        # dataframe -> physical source tables
        self.df_sources: Dict[str, Set[str]] = {}

        # temp_view -> physical source tables
        self.views: Dict[str, Set[str]] = {}

        # lineage edges
        self.lineages: List[dict] = []
        
        self.variables = {}

    # =====================================================
    # Utility
    # =====================================================

    def _extract_root_name(self, node):
    
        if isinstance(node, ast.Name):
            return node.id

        if isinstance(node, ast.Attribute):
            return self._extract_root_name(node.value)

        if isinstance(node, ast.Call):
            return self._extract_root_name(node.func)

        return None
    
    def _resolve_value(self, node):
    
        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Name):
            return self.variables.get(node.id)

        if isinstance(node, ast.JoinedStr):

            parts = []

            for v in node.values:

                if isinstance(v, ast.Constant):
                    parts.append(str(v.value))

                elif isinstance(v, ast.FormattedValue):

                    value = self._resolve_value(v.value)

                    if value:
                        parts.append(str(value))

            return "".join(parts)

        return None
        

    def _get_string(self, node):

        if isinstance(node, ast.Constant):
            return node.value

        return ast.literal_eval(node)

    def _extract_base_df(self, node):

        while isinstance(node, ast.Call):

            if isinstance(node.func, ast.Attribute):
                node = node.func.value
            else:
                break

        if isinstance(node, ast.Name):
            return node.id

        return None

    def _is_physical_table_expr(self, table_expr):

        parts = []

        catalog = getattr(table_expr, "catalog", None)
        db = getattr(table_expr, "db", None)

        if catalog:
            parts.append(catalog)

        if db:
            parts.append(db)

        parts.append(table_expr.name)

        return len(parts) >= 2

    def _build_table_name(self, table_expr):

        parts = []

        catalog = getattr(table_expr, "catalog", None)
        db = getattr(table_expr, "db", None)

        if catalog:
            parts.append(catalog)

        if db:
            parts.append(db)

        parts.append(table_expr.name)

        return ".".join(parts)

    # =====================================================
    # SQL Processing
    # =====================================================
    def _extract_tables_from_sql(self, tree):

        physical_tables = set()
        logical_tables = set()

        cte_names = set()

        with_clause = tree.args.get("with")

        if with_clause:

            for cte in with_clause.find_all(exp.CTE):

                cte_names.add(
                    cte.alias_or_name
                )

        for table in tree.find_all(exp.Table):

            table_name = table.name

            db = table.args.get("db")

            catalog = table.args.get("catalog")

            if table_name in cte_names:
                continue

            if db:

                parts = []

                if catalog:
                    parts.append(catalog.name)

                parts.append(db.name)

                parts.append(table_name)

                physical_tables.add(
                    ".".join(parts)
                )

            else:

                logical_tables.add(
                    table_name
                )
        # print(physical_tables, logical_tables)
        return physical_tables, logical_tables
    

    def _resolve_sql_sources(self, tree):

        physical_tables, logical_tables = (
            self._extract_tables_from_sql(tree)
        )

        result = set(physical_tables)

        for obj in logical_tables:

            if obj in self.views:
                result.update(
                    self.views[obj]
                )

        return result

    def _process_sql_statement(self, tree):

        # -------------------------------------------------
        # CREATE VIEW
        # -------------------------------------------------

        if isinstance(tree, exp.Create):

            try:

                view_name = tree.this.name

                if tree.expression:

                    sources = self._resolve_sql_sources(
                        tree.expression
                    )

                    self.views[view_name] = sources

            except Exception:
                pass

        # -------------------------------------------------
        # INSERT INTO
        # -------------------------------------------------

        elif isinstance(tree, exp.Insert):

            try:

                target = tree.this.name

                db = getattr(tree.this, "db", None)

                if db:
                    target = f"{db}.{target}"

                sources = self._resolve_sql_sources(
                    tree
                )

                sources.discard(target)

                self.lineages.append(
                    {
                        "target": target,
                        "sources": sorted(
                            list(sources)
                        ),
                        "type": "sql_insert",
                    }
                )

            except Exception:
                pass

    def _process_sql_text(self, sql_text):

        try:

            statements = sqlglot.parse(
                sql_text,
                read="spark"
            )

            for stmt in statements:
                self._process_sql_statement(stmt)

        except Exception:
            pass

    # =====================================================
    # Assignment
    # =====================================================

    def visit_Assign(self, node):
        if len(node.targets) != 1:
            return

        if not isinstance(node.targets[0], ast.Name):
            return
        
        # print(ast.dump(node, indent=2))
        
        target_node = node.targets[0]
        df_name = node.targets[0].id
        value = node.value
        # print(type(node.value), (value.func, value.func.attr ) if hasattr(value,'func') else None )
        if isinstance(node.value, ast.Constant):
            # print(df_name, value.value)
            self.variables[df_name] = value.value
            
        if isinstance(target_node, ast.Name):
            value_tmp = self._resolve_value(node.value)
            if isinstance(value_tmp, str):

                self.variables[
                    target_node.id
                ] = value_tmp

        
        # print(type(node.value), (value.func, value.func.attr ) if hasattr(value,'func') else None )
        # -------------------------------------------------
        # spark.table(...)
        # -------------------------------------------------

        if (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "table"
        ):

            try:

                table_name = self._resolve_value(node.args[0])

                if "." in table_name:

                    self.df_sources[df_name] = {
                        table_name
                    }

            except Exception:
                pass

        # -------------------------------------------------
        # spark.sql(...)
        # -------------------------------------------------

        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "sql"
        ):
            try:

                sql_text = self._resolve_value(
                    value.args[0]
                )

                tree = sqlglot.parse_one(
                    sql_text,
                    read="spark"
                )
                # for t in tree.find_all(exp.Table):
                #     print(
                #         "TABLE:",
                #         t,
                #         "NAME=", t.name,
                #         "DB=", getattr(t, "db", None),
                #         "CATALOG=", getattr(t, "catalog", None)
                #     )

                sources = self._resolve_sql_sources(
                    tree
                )

                self.df_sources[df_name] = sources

            except Exception:
                pass

        # -------------------------------------------------
        # dataframe transform chain
        # -------------------------------------------------

        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
        ):

            root_df = self._extract_base_df(
                value.func.value
            )

            if (
                root_df
                and root_df in self.df_sources
            ):

                self.df_sources[df_name] = set(
                    self.df_sources[root_df]
                )

        self.generic_visit(node)

    # =====================================================
    # Function Calls
    # =====================================================

    def visit_Call(self, node):

        # -------------------------------------------------
        # spark.sql(...)
        # -------------------------------------------------

        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
        ):

            try:
                sql_text = self._resolve_value(node.args[0])

                self._process_sql_text(
                    sql_text
                )

            except Exception:
                pass

        # -------------------------------------------------
        # createOrReplaceTempView(...)
        # -------------------------------------------------

        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr
            == "createOrReplaceTempView"
        ):

            try:

                view_name = self._resolve_value(node.args[0])

                df_name = self._extract_base_df(
                    node.func.value
                )

                if (
                    df_name
                    and df_name in self.df_sources
                ):

                    self.views[view_name] = set(
                        self.df_sources[df_name]
                    )

            except Exception:
                pass

        # -------------------------------------------------
        # saveAsTable(...)
        # -------------------------------------------------

        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr
            == "saveAsTable"
        ):
            # print(ast.dump(node, indent=2))
            try:

                target = self._resolve_value(
                    node.args[0]
                )

                df_name = self._extract_root_name(
                    node.func
                )
                # print(df_name)
                # print(node.func)
                if (
                    target
                    and df_name in self.df_sources
                ):

                    self.lineages.append(
                        {
                            "target": target,
                            "sources": sorted(
                                list(self.df_sources[df_name])
                            ),
                            "type": "saveAsTable",
                        }
                    ) 

            except Exception:
                pass
        
        if (
            isinstance(node.func, ast.Attribute)   and node.func.attr    == "save"
        ):
            # print(ast.dump(node, indent=2))
            try:

                target = self._resolve_value(
                    node.args[0]
                )

                df_name = self._extract_root_name(
                    node.func
                )
                # print(df_name)
                # print(node.func)
                if (
                    target
                    and df_name in self.df_sources
                ):

                    self.lineages.append(
                        {
                            "target": target,
                            "sources": sorted(
                                list(self.df_sources[df_name])
                            ),
                            "type": "save",
                        }
                    ) 

            except Exception:
                pass

        # -------------------------------------------------
        # insertInto(...)
        # -------------------------------------------------

        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr
            == "insertInto"
        ):

            try:

                target = self._resolve_value(node.args[0])

                source_df = self._extract_base_df(
                    node.func.value
                )

                sources = self.df_sources.get(
                    source_df,
                    set()
                )

                self.lineages.append(
                    {
                        "target": target,
                        "sources": sorted(
                            list(sources)
                        ),
                        "type": "insertInto",
                    }
                )

            except Exception:
                pass

        self.generic_visit(node)

    # =====================================================
    # Result
    # =====================================================

    def get_lineage(self):

        result = []

        for lineage in self.lineages:

            target = lineage["target"]

            for source in lineage["sources"]:

                result.append(
                    {
                        "from": source,
                        "to": target,
                        "type": lineage["type"],
                    }
                )

        return result