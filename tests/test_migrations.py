import io
import re
from pathlib import Path
from unittest import mock

import pytest
from alembic import command
from alembic.config import Config
from alembic.util import CommandError
from sqlalchemy import create_engine

import ethereumetl
from ethereumetl.clickhouse import (
    SCHEMA_FILE_PATH,
    get_schema_ddls,
    write_schema_ddl,
)
from tests.helpers import run_slow_tests


@pytest.mark.skipif(not run_slow_tests, reason='Skipping slow running tests')
def test_up_down(clickhouse_url):
    alembic_ini = Path(ethereumetl.__file__).parent / '../alembic.ini'
    alembic_cfg = Config(alembic_ini)
    with (
        mock.patch('os.environ', {'CLICKHOUSE_URL': clickhouse_url}),
        create_engine(
            clickhouse_url,
            connect_args={"ch_settings": {'mutations_sync': '1'}},
        ).connect() as connection,
    ):
        command.upgrade(alembic_cfg, 'base')

        stack = []

        # Upgrade to head one by one revision until head is reached and this error is raised:
        # alembic.util.exc.CommandError: Relative revision +1 didn't produce 1 migrations
        with pytest.raises(
            CommandError,
            match=re.escape("Relative revision +1 didn't produce 1 migrations"),
        ):
            for _i in range(999999):
                ddls = list(get_schema_ddls(connection))
                command.upgrade(alembic_cfg, '+1')
                stack.append(ddls)
            raise Exception('Should not reach here')

        while len(stack) > 1:  # don't test downgrading from the initial revision
            command.downgrade(alembic_cfg, '-1')
            before_upgrade_ddls = stack.pop()
            after_downgrade_ddls = list(get_schema_ddls(connection))
            assert after_downgrade_ddls == before_upgrade_ddls


def test_schema_file(clickhouse_migrated_url):
    with create_engine(clickhouse_migrated_url).connect() as connection:
        buf = io.StringIO()
        write_schema_ddl(connection, buf)

    generated_schema_text = buf.getvalue()

    try:
        repo_schema_text = SCHEMA_FILE_PATH.read_text()
    except FileNotFoundError as e:
        raise Exception(f'{e}: Please dump schema') from e

    assert (
        repo_schema_text == generated_schema_text
    ), 'schema.sql is not up to date: please dump schema'
