from textual.app import App, ComposeResult
from textual.widgets import (
    Footer,
    Header,
    Button,
    Label,
    Input,
    TabbedContent,
    TabPane,
    ContentSwitcher,
    Markdown,
    DataTable,
    ProgressBar,
)
from textual.containers import (
    VerticalGroup,
    Horizontal,
    Grid,
    Center,
    VerticalScroll,
)
from textual.screen import ModalScreen
from textual import work
from itertools import count
from typing import Literal
from etl_saphana_athena.config import create_config, load_config
from etl_saphana_athena.load import write_parquet


CON_MARKDOWN = """\
# Conexao SAP/ATHENA

Informa os dados de conexao `SAPHANA` e `ATHENA`.

## Campos

- (SAP) host, port
- (SAP) database
- (SAP) user, password
- (AWS) region
- (AWS) s3_location
- (AWS) aws_access_key_id
- (AWS) aws_secret_access_key
"""

LIST_MARKDOWN = """\
# Tabelas ETL

Informe as tabelas que serão exportadas pro `ATHENA`.

## Dados

- (SAP) schema
- (SAP) table name
- (AWS) schema
- (AWS) table name
- (AWS) append/replace/merge
"""


class DialogScreen(ModalScreen):
    """Screen with a dialog to show a message."""

    DEFAULT_CSS = """
        DialogScreen {
            align: center middle;
        }

        #dialog {
            grid-size: 2;
            grid-gutter: 1 2;
            grid-rows: 1fr 3;
            padding: 0 1;
            width: 65;
            height: 12;
            border: thick $background 80%;
            background: $surface;
        }

        #question {
            column-span: 2;
            height: 1fr;
            width: 1fr;
            content-align: center middle;
        }

        #ok {
            column-span: 2;
            height: 1fr;
            width: 1fr;
            content-align: center middle;
        }
    """

    def __init__(
        self,
        msg: str,
        variant: Literal[
            "default", "primary", "success", "warning", "error"
        ] = "success",
    ) -> None:
        super().__init__()
        self.msg = msg
        self.variant = variant

    def compose(self) -> ComposeResult:
        yield Grid(
            Label(self.msg, id="question"),
            Button("OK", variant=self.variant, id="ok"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "ok":
            self.app.pop_screen()


class Sidebar(VerticalGroup):
    def compose(self) -> ComposeResult:
        yield Button("CONFIGURAR", id="connector", variant="success")
        yield Button("CARREGAR", id="listtable", variant="error")

        with ContentSwitcher(initial="m_connector", id="m_content"):
            yield Markdown(CON_MARKDOWN, id="m_connector")
            yield Markdown(LIST_MARKDOWN, id="m_listtable")


class Connector(VerticalGroup):
    DEFAULT_CSS = """
        Grid {
            background: $boost;
            padding: 1 2;
            height: auto;
            grid-size: 2;
            grid-gutter: 1;
            grid-columns: auto 1fr;
            border: tall black;
            &:focus-within {
                border: tall $accent;
            }
            Label {
                width: 100%;
                text-align: right;
            }
            Button {
                width: 100%;
            }
        }
    """

    COLUMNS = (
        "host",
        "port",
        "username",
        "password",
        "region_aws",
        "s3_location",
        "aws_access_key_id",
        "aws_secret_access_key",
    )

    def load_config(self) -> dict:
        return load_config()

    def compose(self) -> ComposeResult:
        self.config = self.load_config()
        config_sap = self.config.get("sap", dict())
        config_athena = self.config.get("athena", dict())

        with TabbedContent(initial="tabsap", id="tabs"):
            with TabPane("SAPHANA", id="tabsap"):
                with Grid(id="gridsap"):
                    yield Label("host")
                    self.host = Input(placeholder="name host", id="host")
                    if host := config_sap.get("host"):
                        self.host.value = host
                    yield self.host

                    yield Label("port")
                    self.port = Input(placeholder="port", id="port", type="number")
                    if port := config_sap.get("port"):
                        self.port.value = port
                    yield self.port

                    yield Label("user")
                    self.user = Input(placeholder="user", id="user")
                    if user := config_sap.get("username"):
                        self.user.value = user
                    yield self.user

                    yield Label("password")
                    self.password = Input(
                        placeholder="password", id="password", password=True
                    )
                    if password := config_sap.get("password"):
                        self.password.value = password
                    yield self.password

            with TabPane("ATHENA", id="tabathena"):
                with Grid(id="gridathena"):
                    yield Label("region")
                    self.region_aws = Input(placeholder="region", id="region")
                    if region_aws := config_athena.get("region_aws"):
                        self.region_aws.value = region_aws
                    yield self.region_aws

                    yield Label("s3_location")
                    self.s3_location = Input(
                        placeholder="s3 location", id="s3_location"
                    )
                    if s3_location := config_athena.get("s3_location"):
                        self.s3_location.value = s3_location
                    yield self.s3_location

                    yield Label("aws_access_key_id")
                    self.aws_access_key_id = Input(
                        placeholder="aws access key id", id="aws_access_key_id"
                    )
                    if aws_access_key_id := config_athena.get("aws_access_key_id"):
                        self.aws_access_key_id.value = aws_access_key_id
                    yield self.aws_access_key_id

                    yield Label("aws_secret_access_key")
                    self.aws_secret_access_key = Input(
                        placeholder="aws secret access key",
                        id="aws_secret_access_key",
                        password=True,
                    )
                    if aws_secret_access_key := config_athena.get(
                        "aws_secret_access_key"
                    ):
                        self.aws_secret_access_key.value = aws_secret_access_key
                    yield self.aws_secret_access_key

                    yield Label()
                    yield Button("SALVAR", id="btnsalvar", variant="warning")

    def get_values(self) -> list:
        data = [
            self.host.value.strip(),
            self.port.value.strip(),
            self.user.value.strip(),
            self.password.value.strip(),
            self.region_aws.value.strip(),
            self.s3_location.value.strip(),
            self.aws_access_key_id.value.strip(),
            self.aws_secret_access_key.value.strip(),
        ]

        return data


class Tables(VerticalGroup):
    DEFAULT_CSS = """
        DataTable {
            height: 16 !important;
            &.-maximized {
                height: auto !important;
            }
        }
    """

    def __init__(self, columns: tuple[str]) -> None:
        super().__init__()
        self.columns = columns

    def compose(self) -> ComposeResult:
        with Center():
            yield DataTable(fixed_columns=1, id="ref_table")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns(*self.columns)


class ListTables(VerticalGroup):
    DEFAULT_CSS = """
        Grid {
            background: $boost;
            padding: 1 2;
            height: auto;
            grid-size: 2;
            grid-gutter: 1;
            grid-columns: auto 1fr;
            border: tall black;
            &:focus-within {
                border: tall $accent;
            }
            Label {
                width: 100%;
                text-align: right;
            }
            Button {
                width: 100%;
            }
        }

        #btnexportar {
            margin: 1;
            align-horizontal: right;
        }
    """

    COLUMNS = (
        "id",
        "sap_schema",
        "sap_table",
        "aws_schema",
        "aws_table",
        "aws_operation",
    )

    def compose(self) -> ComposeResult:
        with TabbedContent(initial="tabinsert"):
            with TabPane("INSERT", id="tabinsert"):
                with Grid():
                    yield Label("sap schema")
                    self.schema_sap = Input(placeholder="sap schema")
                    yield self.schema_sap

                    yield Label("sap table name")
                    self.table_name_sap = Input(placeholder="sap table name")
                    yield self.table_name_sap

                    yield Label("aws schema")
                    self.schema_aws = Input(placeholder="aws schema")
                    yield self.schema_aws

                    yield Label("aws table name")
                    self.table_name_aws = Input(placeholder="aws table name")
                    yield self.table_name_aws

                    yield Label("aws operation")
                    self.operation_aws = Input(placeholder="aws operation")
                    yield self.operation_aws

                    yield Button("LIMPAR", id="btnlimpar", variant="error")
                    yield Button("INSERIR", id="btninserir", variant="warning")

            with TabPane("TABELA", id="tablist"):
                yield Tables(self.COLUMNS)
                yield Button("EXPORTAR", id="btnexportar", variant="warning")

                with Center():
                    self.progress_bar = ProgressBar(total=1, id="progress")
                    yield self.progress_bar
                    yield Label(id="status")

    def get_values(self) -> list[str]:
        return [
            self.schema_sap.value.strip(),
            self.table_name_sap.value.strip(),
            self.schema_aws.value.strip(),
            self.table_name_aws.value.strip(),
            self.operation_aws.value.strip(),
        ]

    def clear_values(self) -> None:
        self.schema_sap.value = ""
        self.table_name_sap.value = ""
        self.schema_aws.value = ""
        self.table_name_aws.value = ""
        self.operation_aws.value = ""


class EtlSaphanaAthenaApp(App):
    DEFAULT_CSS = """
        #sidebar {
            dock: left;
            width: 40;
            height: 100%;
            padding: 1;
            margin: 1;
            border: dashed $primary 50%;
            align: center top;
        }

        #sidebar Button {
            width: 100%;
            margin: 1;
            align-horizontal: center;
        }

        #body {
            height: 100%;
            border: dashed $primary 50%;
        }
    """

    TITLE = "EL"
    SUB_TITLE = "saphana => athena"
    COUNTER = count(1)

    BINDINGS = [("ctrl+q", "quit", "SAIR")]

    def compose(self) -> ComposeResult:
        with Horizontal(id="sidebar"):
            yield Sidebar()

        with Horizontal(id="body"):
            with ContentSwitcher(initial="connector", id="content"):
                yield Connector(id="connector")

                with VerticalScroll(id="listtable"):
                    self.list_tables = ListTables()
                    yield self.list_tables

        yield Header(id="header")
        yield Footer(id="footer")

    def on_mount(self) -> None:
        self.theme = "dracula"

    @work
    async def update_progress(self, progress_bar: ProgressBar, btn: Button) -> None:
        progress_bar.progress = 0
        status = self.query_one("#status", Label)

        for index in range(self.table.row_count):
            __, schema, table_name, *__ = self.table.get_row_at(index)
            await write_parquet(table_name, schema, status)
            progress_bar.advance(index + 1)

        btn.loading = False
        self.table.loading = False

    def on_button_pressed(self, event: Button.Pressed) -> None:
        id_button = event.button.id

        if id_button in ("connector", "listtable"):
            self.query_one("#content", ContentSwitcher).current = id_button
            self.query_one("#m_content", ContentSwitcher).current = f"m_{id_button}"

        if id_button == "btnsalvar":
            con = self.query_one("#connector", Connector)
            values = con.get_values()

            if all(values):
                config = dict(zip(con.COLUMNS, values))
                create_config(config)
                self.push_screen(DialogScreen("Dados salvo com sucesso !"))

            else:
                self.push_screen(DialogScreen("Validar campos !", variant="error"))

        if id_button == "btnlimpar":
            self.list_tables.clear_values()

        if id_button == "btninserir":
            self.table = self.query_one("#ref_table", DataTable)
            valores = self.list_tables.get_values()

            if all(valores):
                counter = next(self.COUNTER)
                self.table.add_row(str(counter), *valores)

                self.push_screen(DialogScreen("\n".join(valores)))
                self.list_tables.schema_sap.focus()
                self.list_tables.progress_bar.total = self.table.row_count
            else:
                self.push_screen(DialogScreen("Validar campos !", variant="error"))

        if id_button == "btnexportar":
            btn_load = self.query_one(f"#{id_button}", Button)
            btn_load.loading = True
            self.table.loading = True

            self.update_progress(self.list_tables.progress_bar, btn_load)


def main():
    app = EtlSaphanaAthenaApp()
    app.run()
