import os
import pathlib
import pytest
from dataclasses import dataclass
from unittest.mock import MagicMock

from production_scripts_statement_delivery_method_update.delivery_method_update import (
    AppWorxEnum,
    get_config,
    ScriptData,
    get_email_template,
)


@dataclass
class FakeApwxArgs:
    TNS_SERVICE_NAME: str
    CONFIG_FILE_PATH: str
    OUTPUT_FILE_PATH: str
    OUTPUT_FILE_NAME: str
    RUN_DATE: str
    RPTONLY_YN: str
    FULL_CLEANUP_YN: str
    SEND_EMAIL_YN: str
    EMAIL_RECIPIENTS: str
    SMTP_SERVER: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_PASSWORD: str
    FROM_EMAIL_ADDR: str
    TEST_EMAIL_ADDR: str


@dataclass
class FakeApwx:
    args: FakeApwxArgs


TEST_BASE_PATH = pathlib.Path(os.path.dirname(__file__))
CONFIG_PATH = TEST_BASE_PATH.parent / "config" / "config.yaml"


SCRIPT_ARGUMENTS = {
    str(AppWorxEnum.TNS_SERVICE_NAME): "FAKE_DB",
    str(AppWorxEnum.CONFIG_FILE_PATH): str(CONFIG_PATH),
    str(AppWorxEnum.OUTPUT_FILE_PATH): str(TEST_BASE_PATH),
    str(AppWorxEnum.OUTPUT_FILE_NAME): "delivery_method_update_report.csv",
    str(AppWorxEnum.RUN_DATE): "01-15-2024",
    str(AppWorxEnum.RPTONLY_YN): "N",
    str(AppWorxEnum.FULL_CLEANUP_YN): "N",
    str(AppWorxEnum.SEND_EMAIL_YN): "Y",
    str(AppWorxEnum.EMAIL_RECIPIENTS): "test@firsttechfed.com",
    str(AppWorxEnum.SMTP_SERVER): "smtp.test.com",
    str(AppWorxEnum.SMTP_PORT): 587,
    str(AppWorxEnum.SMTP_USER): "test_user",
    str(AppWorxEnum.SMTP_PASSWORD): "test_password",
    str(AppWorxEnum.FROM_EMAIL_ADDR): "AM_PROD@firsttechfed.com",
    str(AppWorxEnum.TEST_EMAIL_ADDR): "test@firsttechfed.com",
}

SCRIPT_ARGUMENTS_REPORT_ONLY = {
    **SCRIPT_ARGUMENTS,
    str(AppWorxEnum.OUTPUT_FILE_NAME): "delivery_method_update_report_only.csv",
    str(AppWorxEnum.RPTONLY_YN): "Y",
    str(AppWorxEnum.SEND_EMAIL_YN): "N",
}

SCRIPT_ARGUMENTS_FULL_CLEANUP = {
    **SCRIPT_ARGUMENTS,
    str(AppWorxEnum.OUTPUT_FILE_NAME): "delivery_method_update_full_cleanup.csv",
    str(AppWorxEnum.RUN_DATE): None,
    str(AppWorxEnum.FULL_CLEANUP_YN): "Y",
}


def new_fake_apwx(script_args: dict) -> FakeApwx:
    return FakeApwx(
        args=FakeApwxArgs(
            TNS_SERVICE_NAME=script_args[str(AppWorxEnum.TNS_SERVICE_NAME)],
            CONFIG_FILE_PATH=script_args[str(AppWorxEnum.CONFIG_FILE_PATH)],
            OUTPUT_FILE_PATH=script_args[str(AppWorxEnum.OUTPUT_FILE_PATH)],
            OUTPUT_FILE_NAME=script_args[str(AppWorxEnum.OUTPUT_FILE_NAME)],
            RUN_DATE=script_args[str(AppWorxEnum.RUN_DATE)],
            RPTONLY_YN=script_args[str(AppWorxEnum.RPTONLY_YN)],
            FULL_CLEANUP_YN=script_args[str(AppWorxEnum.FULL_CLEANUP_YN)],
            SEND_EMAIL_YN=script_args[str(AppWorxEnum.SEND_EMAIL_YN)],
            EMAIL_RECIPIENTS=script_args[str(AppWorxEnum.EMAIL_RECIPIENTS)],
            SMTP_SERVER=script_args[str(AppWorxEnum.SMTP_SERVER)],
            SMTP_PORT=script_args[str(AppWorxEnum.SMTP_PORT)],
            SMTP_USER=script_args[str(AppWorxEnum.SMTP_USER)],
            SMTP_PASSWORD=script_args[str(AppWorxEnum.SMTP_PASSWORD)],
            FROM_EMAIL_ADDR=script_args[str(AppWorxEnum.FROM_EMAIL_ADDR)],
            TEST_EMAIL_ADDR=script_args[str(AppWorxEnum.TEST_EMAIL_ADDR)],
        )
    )


@pytest.fixture(scope="module")
def script_data():
    apwx = new_fake_apwx(SCRIPT_ARGUMENTS)
    config = get_config(apwx)
    email_template = get_email_template(config)
    dbh = MagicMock()
    return ScriptData(apwx=apwx, dbh=dbh, config=config, email_template=email_template)


@pytest.fixture(scope="module")
def script_data_report_only():
    apwx = new_fake_apwx(SCRIPT_ARGUMENTS_REPORT_ONLY)
    config = get_config(apwx)
    email_template = get_email_template(config)
    dbh = MagicMock()
    return ScriptData(apwx=apwx, dbh=dbh, config=config, email_template=email_template)


@pytest.fixture(scope="module")
def script_data_full_cleanup():
    apwx = new_fake_apwx(SCRIPT_ARGUMENTS_FULL_CLEANUP)
    config = get_config(apwx)
    email_template = get_email_template(config)
    dbh = MagicMock()
    return ScriptData(apwx=apwx, dbh=dbh, config=config, email_template=email_template)


@pytest.fixture
def sample_pers_records():
    return [
        {
            'ENTITY_NUMBER': 123456,
            'ACCTNBR': 'ACC001',
            'ENTITY_TYPE': 'pers',
            'CLOSE_DATE': '2024-01-15'
        },
        {
            'ENTITY_NUMBER': 789012,
            'ACCTNBR': 'ACC002',
            'ENTITY_TYPE': 'pers',
            'CLOSE_DATE': '2024-01-15'
        }
    ]


@pytest.fixture
def sample_org_records():
    return [
        {
            'ENTITY_NUMBER': 345678,
            'ACCTNBR': 'ACC003',
            'ENTITY_TYPE': 'org',
            'CLOSE_DATE': '2024-01-15'
        },
        {
            'ENTITY_NUMBER': 901234,
            'ACCTNBR': 'ACC004',
            'ENTITY_TYPE': 'org',
            'CLOSE_DATE': '2024-01-15'
        }
    ]


@pytest.fixture
def sample_success_records():
    return [
        (123456, 'ACC001', 'pers', '2024-01-15', 'Success'),
        (789012, 'ACC002', 'pers', '2024-01-15', 'Success'),
        (345678, 'ACC003', 'org', '2024-01-15', 'Success'),
    ]


@pytest.fixture
def sample_fail_records():
    return [
        (901234, 'ACC004', 'org', '2024-01-15', 'Fail'),
    ]