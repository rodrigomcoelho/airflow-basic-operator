from os.path import dirname, join
from tempfile import NamedTemporaryFile
from typing import Optional

from airflow.operators.bash import BashOperator
from airflow.utils.context import Context
from jinja2 import Environment

TEMPLATE_FILE_PATH = join(dirname(__file__), "templates", "pandas_to_bigquery.j2")
BASH_COMMAND = "python {python_file}"
PYTHON_EXTENSION = ".py"


class PandasToBigQueryOperator(BashOperator):
    ui_color: str = "#836FFF"
    ui_fgcolor: str = "#FFF"

    def __init__(
        self,
        source_code_path: str,
        destination_table: str,
        location: str,
        write_mode: str,
        project_id: str,
        chunksize: Optional[int] = None,
        *args,
        **kwargs
    ):
        self._source_code_path = source_code_path
        self._destination_table = destination_table
        self._location = location
        self._chunksize = chunksize
        self._write_mode = write_mode
        self._project_id = project_id
        super().__init__(bash_command="", *args, **kwargs)

    @staticmethod
    def read_file_content(file_path) -> str:
        with open(file_path) as content:
            return content.read()

    def write_rendered_file(self, file_path: str, content: str) -> str:
        self.log.info("Escrevendo o arquivo no caminho '%s'.", file_path)

        with open(file_path, "w") as file:
            file.write(content)

        self.log.info("Template criado com sucesso.")

    def render_pandas_bigquery_template(
        self, python_file_path: str, template: str, source_code: str
    ) -> None:
        environment = Environment()

        renderer = environment.from_string(template)

        self.write_rendered_file(
            python_file_path,
            renderer.render(
                sourceCode=source_code,
                destinationTable=self._destination_table,
                chunksize=self._chunksize,
                location=self._location,
                writeMode=self._write_mode,
                projectId=self._project_id,
            ),
        )

    def execute(self, context: Context) -> None:
        source_code = self.read_file_content(self._source_code_path)
        template = self.read_file_content(TEMPLATE_FILE_PATH)

        try:
            python_file = NamedTemporaryFile(suffix=PYTHON_EXTENSION)
            self.render_pandas_bigquery_template(
                python_file_path=python_file.name,
                template=template,
                source_code=source_code,
            )
            self.bash_command = BASH_COMMAND.format(python_file=python_file.name)
            super().execute(context)
        except OSError as error:
            self.log.exception(error)
        finally:
            python_file.flush()
            python_file.close()
