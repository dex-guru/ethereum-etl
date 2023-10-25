import io
import re
import shutil
from pathlib import Path
from unittest import mock

import pytest
from alembic import command
from alembic.config import Config
from alembic.util import CommandError

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
    with mock.patch('os.environ', {'CLICKHOUSE_URL': clickhouse_url}):
        command.upgrade(alembic_cfg, 'base')

        stack = []

        # Upgrade to head one by one revision until head is reached and this error is raised:
        # alembic.util.exc.CommandError: Relative revision +1 didn't produce 1 migrations
        with pytest.raises(
            CommandError,
            match=re.escape("Relative revision +1 didn't produce 1 migrations"),
        ):
            for _i in range(999999):
                ddls = list(get_schema_ddls(clickhouse_url))
                command.upgrade(alembic_cfg, '+1')
                stack.append(ddls)
            raise Exception('Should not reach here')

        while len(stack) > 1:  # don't test downgrading from the initial revision
            command.downgrade(alembic_cfg, '-1')
            before_upgrade_ddls = stack.pop()
            after_downgrade_ddls = list(get_schema_ddls(clickhouse_url))
            assert after_downgrade_ddls == before_upgrade_ddls


def test_schema_file(clickhouse_migrated_url):
    buf = io.StringIO()
    write_schema_ddl(clickhouse_migrated_url, buf)

    generated_schema_text = buf.getvalue()

    try:
        repo_schema_text = SCHEMA_FILE_PATH.read_text()
    except FileNotFoundError as e:
        raise Exception(f'{e}: Please dump schema') from e

    assert (
        repo_schema_text == generated_schema_text
    ), 'schema.sql is not up to date: please dump schema'


def test_new_revision_template(clickhouse_migrated_url, tmp_path):
    shutil.copytree(Path(ethereumetl.__file__).parent / '../db', tmp_path / 'db')
    shutil.copy(Path(ethereumetl.__file__).parent / '../alembic.ini', tmp_path / 'alembic.ini')
    alembic_ini = tmp_path / 'alembic.ini'
    migrations_path = tmp_path / 'db/migrations'
    alembic_cfg = Config(alembic_ini)
    alembic_cfg.set_main_option('script_location', str(migrations_path))

    with mock.patch('os.environ', {'CLICKHOUSE_URL': clickhouse_migrated_url}):
        revision_description = 'bymacgu4'
        command.revision(alembic_cfg, revision_description)

        revision_file_path = next(migrations_path.rglob(f'*{revision_description}*.py'))
        revision = revision_file_path.name.split('_')[1]

        command.upgrade(alembic_cfg, revision)
        command.downgrade(alembic_cfg, f'{revision}-1')
