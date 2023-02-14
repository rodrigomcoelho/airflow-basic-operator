from os.path import dirname, join
from tempfile import NamedTemporaryFile
from typing import Optional

from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.context import Context
from jinja2 import Environment

TEMPLATE_FILE_PATH = join(dirname(__file__), "templates", "pandas_to_bigquery.j2")
BASH_COMMAND = "python {python_file}"
PYTHON_EXTENSION = ".py"


class BurnwoodTableNotFound(Exception):
    ...


class PandasToBigQueryOperator(BashOperator):
    ui_color: str = "#836FFF"
    ui_fgcolor: str = "#FFF"

    def __init__(
        self,
        source_code_path: str,
        destination_dataset: str,
        destination_table: str,
        location: str,
        write_mode: str,
        project_id: str,
        chunksize: Optional[int] = None,
        *args,
        **kwargs,
    ):
        self._source_code_path = source_code_path
        self._destination_table = destination_table
        self._destination_dataset = destination_dataset
        self._location = location
        self._chunksize = chunksize
        self._write_mode = write_mode
        self._project_id = project_id
        super().__init__(bash_command="", *args, **kwargs)

    @staticmethod
    def read_file_content(file_path) -> str:
        with open(file_path) as content:
            return content.read()

    def get_schema(self) -> str:
        bq_hook = BigQueryHook(location=self._location)

        table_schema = bq_hook.get_schema(
            dataset_id=self._destination_dataset,
            table_id=self._destination_table,
            project_id=self._project_id,
        )

        if not table_schema:
            raise BurnwoodTableNotFound(f"Table {self._destination_table} not found.")

        return table_schema

    def render_pandas_bigquery_template(self, template: str, **kwargs) -> str:
        environment = Environment()
        renderer = environment.from_string(template)
        return renderer.render(**kwargs)

    def write_rendered_file(self, file_path: str, content: str) -> str:

        with open(file_path, "w") as file:
            file.write(content)

        self.log.info("Template is successfully written at '%s'.", file_path)


    def execute(self, context: Context) -> None:
        source_code = self.read_file_content(self._source_code_path)

        template = self.read_file_content(TEMPLATE_FILE_PATH)

        rendered_template = self.render_pandas_bigquery_template(
                template=template,
                sourceCode=source_code,
                destinationTable=self._destination_table,
                chunksize=self._chunksize,
                location=self._location,
                writeMode=self._write_mode,
                projectId=self._project_id,
                dataset=self._destination_dataset,
                tableSchema=self.get_schema(),
            )

        try:
            python_file = NamedTemporaryFile(suffix=PYTHON_EXTENSION)

            self.write_rendered_file(
                python_file.name,
                rendered_template,
            )
            self.bash_command = BASH_COMMAND.format(python_file=python_file.name)
            super().execute(context)
        except OSError as error:
            self.log.exception(error)
        finally:
            python_file.flush()
            python_file.close()
