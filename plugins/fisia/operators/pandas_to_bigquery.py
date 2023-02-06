from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from typing import Optional
from os.path import dirname, join
from jinja2 import Environment

TEMPLATE_FILE_PATH = join(dirname(__file__), "templates", "pandas_to_bigquery.j2")

class PandasToBigQueryOperator(BaseOperator):

    ui_color: str = "#836FFF"
    ui_fgcolor: str = "#FFF"

    def __init__(
        self,
        source_code_path: str,
        destination_table: str,
        location: str,
        chunksize: Optional[int] = None,
        *args,
        **kwargs
    ):
        self._source_code_path = source_code_path
        self._destination_table = destination_table
        self._location = location
        self._chunksize = chunksize
        super().__init__(*args, **kwargs)

    @staticmethod
    def read_file_content(file_path) -> str:
        with open(file_path) as content:
            return content.read()

    def render_pandas_bigquery_template(self, template: str, source_code: str) -> None:
        ...

    def execute(self, context: Context) -> None:
        source_code = self.read_file_content(self._source_code_path)
        template = self.read_file_content(TEMPLATE_FILE_PATH)
