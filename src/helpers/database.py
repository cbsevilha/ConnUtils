import os
from contextlib import contextmanager
import numpy as np
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy import select

import logging

from dotenv import load_dotenv

load_dotenv(override=True)  # take environment variables from .env.


class SQLChunkReaderError(Exception):
    pass


class DBConnectorFactory:
    def __init__(self) -> None:
        self.supported_connectors = ["ANALYTICS_DB", "LOCAL_DB"]

    def get_db_connector(self, db_instance_name, verbose=False, echo=False):
        if db_instance_name.upper() in self.supported_connectors:
            uparams = dict(
                drivername=os.getenv(f"{db_instance_name.upper()}_DRIVER"),
                username=os.getenv(f"{db_instance_name.upper()}_USER"),
                password=os.getenv(f"{db_instance_name.upper()}_PASS"),
                host=os.getenv(f"{db_instance_name.upper()}_HOST"),
                port=os.getenv(f"{db_instance_name.upper()}_PORT"),
                database=os.getenv(f"{db_instance_name.upper()}_DATABASE"),
            )
            schema = os.getenv(f"{db_instance_name.upper()}_SCHEMA")
        else:
            raise NotImplementedError
        return Database(
            url_params=uparams,
            schema=schema,
            verbose=verbose,
            echo=echo
        )


class Database:
    def __init__(self, url_params, schema, verbose=False, echo=False):

        self._queries = None
        self.URL = URL.create(**url_params)
        self.schema = schema
        self.verbose = verbose
        self.logger = logging.getLogger("Database")
        self.engine = create_engine(
            self.URL,
            echo=echo,
            encoding="utf-8",
            poolclass=NullPool,
            future=True,  # this is for SQLAlchemy 2.0
            connect_args={"options": "-c timezone=utc"},
        )
        metadata = MetaData(bind=self.engine)
        metadata.reflect(bind=self.engine, schema=self.schema)
        Base = automap_base(metadata=metadata)
        Base.prepare()
        self.base = Base
        if self.verbose:
            msg = f"Database {self.URL.database} " f"has the following tables:"
            self.logger.info(msg)
            for tbl in Base.classes.keys():
                self.logger.info(tbl)

    def get_session(self, dispose=True):
        if dispose:
            self.engine.dispose()

        Session = sessionmaker(bind=self.engine)
        return Session

    @contextmanager
    def session_scope(self):
        """Context Manager aproach for using
        Session per offical SQL Alchemy Docs"""
        session = self.get_session(dispose=True)()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_sql(self, sql_text, sql_params=None):
        with self.session_scope() as session:
            if sql_params is None:
                result = session.execute(statement=text(sql_text))
            else:
                result = session.execute(
                    statement=text(sql_text),
                    params=sql_params
                )

            res_df = pd.DataFrame(
                data=result.fetchall(),
                columns=result.keys()
            )
            if res_df.shape == (1, 1):
                return res_df.squeeze()
            elif res_df.empty:
                return None
            else:
                return res_df

    def build_in_clause_query(
            self,
            table_name,
            colname,
            in_list,
            is_string=True
    ):
        if is_string:
            in_list = "', '".join(in_list)
            q = (
                f"SELECT * "
                f"FROM {self.schema}.{table_name} "
                f"WHERE {colname} IN ('{in_list}');"
            )
        else:
            in_list = ", ".join(in_list)
            q = (
                f"SELECT * FROM "
                f"{self.schema}.{table_name} "
                f"WHERE {colname} IN ({in_list});"
            )

        return q

    def run_in_clause_query(
            self, table_name, colname, in_list, is_string=True, chunk_size=200
    ):
        chunks_list = [
            in_list[i: i + chunk_size]
            for i in range(0,
                           len(in_list),
                           chunk_size)
        ]

        df_list = list()
        for ch in chunks_list:
            df_res = self.execute_sql(
                sql_text=self.build_in_clause_query(
                    table_name=table_name,
                    colname=colname,
                    in_list=ch,
                    is_string=is_string,
                )
            )
            df_list.append(df_res)

        final_df = pd.concat(objs=df_list, axis=0, ignore_index=True)
        return final_df

    def get_full_table_data(self, table_name):
        return self.select_query(table_name=table_name)
        # return self.execute_sql
        # (sql_text=f'SELECT * FROM {table_name};', sql_params=None)

    def get_table_object(self, table_name):
        return self.base.classes[table_name]

    def select_query(
            self,
            table_name,
            filter_dict=None,
            feedback_cols_list=None
    ):
        with self.session_scope() as session:
            table_obj = self.get_table_object(table_name)
            stmt = select(table_obj)
            if filter_dict is not None:
                stmt = stmt.filter_by(**filter_dict)
            qres = session.execute(statement=stmt).all()
            if len(qres) == 0:
                return None
            else:
                rlist = list()
                for r in qres:
                    tmpd = dict()
                    if feedback_cols_list is None:
                        feedback_cols_list = list(
                            table_obj.__table__._columns.keys()
                        )
                    for k in feedback_cols_list:
                        tmpd[k] = getattr(r[table_name], k)
                    rlist.append(tmpd)

                res_df = pd.DataFrame(rlist)
                if res_df.shape == (1, 1):
                    return res_df.squeeze()
                else:
                    return res_df

    def insert_row(
            self,
            table_name,
            row_dict,
            feedback_cols_list=None
    ):
        table_obj = self.get_table_object(table_name)
        table_instance = table_obj(**row_dict)
        with self.session_scope() as session:
            stmt = select(table_obj).filter_by(**row_dict)
            qres = session.execute(statement=stmt).all()
            if len(qres) == 0:
                if feedback_cols_list is None:
                    session.add(table_instance)
                else:
                    session.add(table_instance)
                    session.flush()
                    feedback_dict = {
                        col: table_instance.__dict__[col]
                        for col in feedback_cols_list
                    }
                    # this might not be necessary since the
                    # context manager does commit already
                    session.commit()
                    return feedback_dict
            elif len(qres) == 1:
                rlist = list()
                for r in qres:
                    tmpd = dict()
                    if feedback_cols_list is None:
                        feedback_cols_list = list(
                            table_obj.__table__._columns.keys()
                        )
                    for k in feedback_cols_list:
                        tmpd[k] = getattr(r[table_name], k)
                    rlist.append(tmpd)

                return rlist[0]
            else:
                msg = "there is more than 1 record match " \
                      "for the supplied filter."
                self.logger.error(msg)
                raise ValueError

    def pg_insert_ignore_chunks(
            self,
            table_name,
            df,
            chunk_size=500,
            verbose=False
    ):
        df = df.copy()
        df = df.replace(
            {pd.NaT: None}
        ).replace(
            {"NaT": None}
        ).replace({np.NaN: None})
        df = df.where((pd.notnull(df)), None)
        df = df.where((pd.notna(df)), None)

        ins_list = df.to_dict(orient="records")
        tbl_obj = self.get_table_object(table_name=table_name)

        chunks_list = [
            ins_list[i: i + chunk_size]
            for i in range(0,
                           len(ins_list),
                           chunk_size)
        ]

        rec_len = 0
        for ch in chunks_list:
            if verbose:
                msg = f"Inserting records: {rec_len + 1:,}"
                msg += f"through {rec_len + len(ch):,}"
                self.logger.info(msg)
            rec_len += len(ch)
            stmt = postgres_insert(tbl_obj).values(ch).on_conflict_do_nothing()
            with self.session_scope() as session:
                session.execute(statement=stmt)
