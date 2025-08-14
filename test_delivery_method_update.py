import datetime
import time
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from delivery_method_update import (
    run,
    initialize,
    fetch_records,
    process_records,
    update_stdl_userfield,
    write_report_file,
    send_notification_email,
    execute_sql_select,
    dna_db_connect,
    AppWorxEnum,
)

REPORT_EPOCH_TIMESTAMP = time.mktime(datetime.datetime(2024, 1, 15, 0, 0, 0).timetuple())
MODULE_NAME = os.path.basename(Path(os.path.dirname(__file__)).parent)

# Sample database records for testing
SAMPLE_DB_RECORDS = [
    {
        "ENTITY_NUMBER": 123456,
        "ACCTNBR": "ACC001",
        "ENTITY_TYPE": "pers",
        "CLOSE_DATE": "2024-01-15"
    },
    {
        "ENTITY_NUMBER": 789012,
        "ACCTNBR": "ACC002", 
        "ENTITY_TYPE": "pers",
        "CLOSE_DATE": "2024-01-15"
    },
    {
        "ENTITY_NUMBER": 345678,
        "ACCTNBR": "ACC003",
        "ENTITY_TYPE": "org",
        "CLOSE_DATE": "2024-01-15"
    },
    {
        "ENTITY_NUMBER": 901234,
        "ACCTNBR": "ACC004",
        "ENTITY_TYPE": "org", 
        "CLOSE_DATE": "2024-01-15"
    }
]

# Expected success records
EXPECTED_SUCCESS_RECORDS = [
    (123456, 'ACC001', 'pers', '2024-01-15', 'Success'),
    (789012, 'ACC002', 'pers', '2024-01-15', 'Success'),
    (345678, 'ACC003', 'org', '2024-01-15', 'Success'),
    (901234, 'ACC004', 'org', '2024-01-15', 'Success'),
]

# Expected failure records
EXPECTED_FAIL_RECORDS = [
    (901234, 'ACC004', 'org', '2024-01-15', 'Fail'),
]


def test_fetch_records_with_run_date(script_data):
    """Test fetching records with specific run date"""
    with patch('delivery_method_update.execute_sql_select') as mock_execute:
        mock_execute.return_value = SAMPLE_DB_RECORDS
        
        pers_records, org_records = fetch_records(script_data)
        
        assert len(pers_records) == 2
        assert len(org_records) == 2
        assert all(r['ENTITY_TYPE'] == 'pers' for r in pers_records)
        assert all(r['ENTITY_TYPE'] == 'org' for r in org_records)
        mock_execute.assert_called_once()


def test_fetch_records_with_full_cleanup(script_data_full_cleanup):
    """Test fetching records with full cleanup mode"""
    with patch('delivery_method_update.execute_sql_select') as mock_execute:
        mock_execute.return_value = SAMPLE_DB_RECORDS
        
        pers_records, org_records = fetch_records(script_data_full_cleanup)
        
        assert len(pers_records) == 2
        assert len(org_records) == 2
        mock_execute.assert_called_once()


def test_fetch_records_parameter_validation_both_provided(script_data):
    """Test parameter validation when both RUN_DATE and FULL_CLEANUP_YN are provided"""
    script_data.apwx.args.RUN_DATE = "01-15-2024"
    script_data.apwx.args.FULL_CLEANUP_YN = "Y"
    
    try:
        fetch_records(script_data)
        assert False, "Should have raised exception"
    except Exception as e:
        assert "mutually exclusive" in str(e)


def test_fetch_records_parameter_validation_neither_provided(script_data):
    """Test parameter validation when neither parameter is provided"""
    script_data.apwx.args.RUN_DATE = None
    script_data.apwx.args.FULL_CLEANUP_YN = "N"
    
    try:
        fetch_records(script_data)
        assert False, "Should have raised exception"
    except Exception as e:
        assert "no RUN_DATE parameter provided" in str(e)


def test_process_records_success(script_data, sample_pers_records, sample_org_records):
    """Test successful processing of records"""
    with patch('delivery_method_update.update_stdl_userfield') as mock_update, \
         patch('pathlib.Path.exists') as mock_exists:
        
        mock_exists.return_value = False
        mock_update.side_effect = [
            (EXPECTED_SUCCESS_RECORDS[:2], []),  # person records
            (EXPECTED_SUCCESS_RECORDS[2:], [])   # org records
        ]
        
        successes, fails = process_records(script_data, sample_pers_records, sample_org_records)
        
        assert len(successes) == 4
        assert len(fails) == 0
        assert mock_update.call_count == 2


def test_process_records_with_failures(script_data, sample_pers_records, sample_org_records):
    """Test processing records with some failures"""
    with patch('delivery_method_update.update_stdl_userfield') as mock_update, \
         patch('pathlib.Path.exists') as mock_exists:
        
        mock_exists.return_value = False
        mock_update.side_effect = [
            (EXPECTED_SUCCESS_RECORDS[:2], []),  # person records - all success
            (EXPECTED_SUCCESS_RECORDS[2:3], EXPECTED_FAIL_RECORDS)  # org records - one failure
        ]
        
        successes, fails = process_records(script_data, sample_pers_records, sample_org_records)
        
        assert len(successes) == 3
        assert len(fails) == 1
        assert mock_update.call_count == 2


def test_update_stdl_userfield_success(script_data, sample_pers_records):
    """Test successful update of STDL userfield"""
    mock_cursor = MagicMock()
    mock_cursor.getbatcherrors.return_value = []
    mock_cursor.rowcount = 2
    script_data.dbh.cursor.return_value = mock_cursor
    
    successes, fails = update_stdl_userfield(script_data, sample_pers_records, 'persuserfield', 'persnbr')
    
    assert len(successes) == 2
    assert len(fails) == 0
    mock_cursor.executemany.assert_called_once()


def test_update_stdl_userfield_with_batch_errors(script_data, sample_pers_records):
    """Test update with batch errors"""
    mock_cursor = MagicMock()
    mock_error = MagicMock()
    mock_error.offset = 0
    mock_error.message = "Test error"
    mock_cursor.getbatcherrors.return_value = [mock_error]
    mock_cursor.rowcount = 1
    script_data.dbh.cursor.return_value = mock_cursor
    
    successes, fails = update_stdl_userfield(script_data, sample_pers_records, 'persuserfield', 'persnbr')
    
    assert len(fails) > 0
    mock_cursor.executemany.assert_called_once()


def test_write_report_file(script_data, sample_success_records, sample_fail_records):
    """Test writing report file with both successes and failures"""
    with patch('delivery_method_update.write_report') as mock_write_report:
        write_report_file(script_data, sample_success_records, sample_fail_records)
        
        assert mock_write_report.call_count == 2
        # Verify write mode for successes and append mode for failures
        calls = mock_write_report.call_args_list
        assert calls[0][1]['write_mode'] == 'w'
        assert calls[1][1]['write_mode'] == 'a+'


def test_send_notification_email_with_failures(script_data, sample_fail_records):
    """Test sending notification email when there are failures"""
    with patch('delivery_method_update.send_email') as mock_send_email:
        mock_send_email.return_value = (True, "Email Sent")
        
        send_notification_email(script_data, sample_fail_records)
        
        mock_send_email.assert_called_once()


def test_send_notification_email_no_failures(script_data):
    """Test not sending notification email when there are no failures"""
    with patch('delivery_method_update.send_email') as mock_send_email:
        send_notification_email(script_data, [])
        
        mock_send_email.assert_not_called()


def test_run_normal_mode(script_data, mocker):
    """Test the main run function in normal mode"""
    mocker.patch(f"delivery_method_update.initialize", return_value=script_data)
    mocker.patch(f"delivery_method_update.fetch_records", return_value=(
        [r for r in SAMPLE_DB_RECORDS if r['ENTITY_TYPE'] == 'pers'],
        [r for r in SAMPLE_DB_RECORDS if r['ENTITY_TYPE'] == 'org']
    ))
    mock_process = mocker.patch(f"delivery_method_update.process_records")
    mock_process.return_value = (EXPECTED_SUCCESS_RECORDS, [])
    mock_write = mocker.patch(f"delivery_method_update.write_report_file")
    mock_email = mocker.patch(f"delivery_method_update.send_notification_email")
    
    result = run(script_data.apwx)
    
    assert result is True
    mock_process.assert_called_once()
    mock_write.assert_called_once()
    mock_email.assert_called_once()
    script_data.dbh.close.assert_called_once()


def test_run_report_only_mode(script_data_report_only, mocker):
    """Test the main run function in report-only mode"""
    mocker.patch(f"delivery_method_update.initialize", return_value=script_data_report_only)
    mocker.patch(f"delivery_method_update.fetch_records", return_value=(
        [r for r in SAMPLE_DB_RECORDS if r['ENTITY_TYPE'] == 'pers'],
        [r for r in SAMPLE_DB_RECORDS if r['ENTITY_TYPE'] == 'org']
    ))
    mock_process = mocker.patch(f"delivery_method_update.process_records")
    mock_process.return_value = (EXPECTED_SUCCESS_RECORDS, [])
    mock_write = mocker.patch(f"delivery_method_update.write_report_file")
    mock_email = mocker.patch(f"delivery_method_update.send_notification_email")
    
    result = run(script_data_report_only.apwx)
    
    assert result is True
    mock_process.assert_called_once()
    mock_write.assert_called_once()
    mock_email.assert_called_once()
    script_data_report_only.dbh.close.assert_called_once()


def test_run_with_failures(script_data, mocker):
    """Test the main run function when there are processing failures"""
    mocker.patch(f"delivery_method_update.initialize", return_value=script_data)
    mocker.patch(f"delivery_method_update.fetch_records", return_value=(
        [r for r in SAMPLE_DB_RECORDS if r['ENTITY_TYPE'] == 'pers'],
        [r for r in SAMPLE_DB_RECORDS if r['ENTITY_TYPE'] == 'org']
    ))
    mock_process = mocker.patch(f"delivery_method_update.process_records")
    mock_process.return_value = (EXPECTED_SUCCESS_RECORDS[:3], EXPECTED_FAIL_RECORDS)
    mock_write = mocker.patch(f"delivery_method_update.write_report_file")
    mock_email = mocker.patch(f"delivery_method_update.send_notification_email")
    
    result = run(script_data.apwx)
    
    assert result is True
    mock_process.assert_called_once()
    mock_write.assert_called_once_with(script_data, EXPECTED_SUCCESS_RECORDS[:3], EXPECTED_FAIL_RECORDS)
    mock_email.assert_called_once_with(script_data, EXPECTED_FAIL_RECORDS)
    script_data.dbh.close.assert_called_once()


def test_run_no_data(script_data, mocker):
    """Test the main run function when no data is found"""
    mocker.patch(f"delivery_method_update.initialize", return_value=script_data)
    mocker.patch(f"delivery_method_update.fetch_records", return_value=([], []))
    mock_process = mocker.patch(f"delivery_method_update.process_records")
    mock_process.return_value = ([], [])
    mock_write = mocker.patch(f"delivery_method_update.write_report_file")
    mock_email = mocker.patch(f"delivery_method_update.send_notification_email")
    
    result = run(script_data.apwx)
    
    assert result is True
    mock_process.assert_called_once()
    mock_write.assert_called_once()
    mock_email.assert_called_once()
    script_data.dbh.close.assert_called_once()


def test_dna_db_connect_normal_mode(script_data):
    """Test database connection in normal mode (RPTONLY_YN = N)"""
    with patch.object(script_data.apwx, 'db_connect') as mock_db_connect:
        mock_conn = MagicMock()
        mock_db_connect.return_value = mock_conn
        
        result = dna_db_connect(script_data.apwx)
        
        assert result == mock_conn
        assert mock_conn.autocommit is True
        mock_db_connect.assert_called_once_with(autocommit=False)


def test_dna_db_connect_report_mode(script_data_report_only):
    """Test database connection in report-only mode (RPTONLY_YN = Y)"""
    with patch.object(script_data_report_only.apwx, 'db_connect') as mock_db_connect:
        mock_conn = MagicMock()
        mock_db_connect.return_value = mock_conn
        
        result = dna_db_connect(script_data_report_only.apwx)
        
        assert result == mock_conn
        assert mock_conn.autocommit is False
        mock_db_connect.assert_called_once_with(autocommit=False)


def test_execute_sql_select_success(script_data):
    """Test successful SQL execution"""
    mock_cursor = MagicMock()
    mock_cursor.description = [('ENTITY_NUMBER',), ('ACCTNBR',), ('ENTITY_TYPE',), ('CLOSE_DATE',)]
    mock_cursor.fetchall.return_value = [
        (123456, 'ACC001', 'pers', '2024-01-15'),
        (789012, 'ACC002', 'pers', '2024-01-15')
    ]
    script_data.dbh.cursor.return_value.__enter__.return_value = mock_cursor
    
    result = execute_sql_select(script_data.dbh, "SELECT * FROM test")
    
    assert len(result) == 2
    assert result[0]['ENTITY_NUMBER'] == 123456
    assert result[0]['ACCTNBR'] == 'ACC001'
    assert result[1]['ENTITY_NUMBER'] == 789012
    assert result[1]['ACCTNBR'] == 'ACC002'
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test", None)


def test_execute_sql_select_with_exception(script_data):
    """Test SQL execution with exception"""
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Database connection error")
    script_data.dbh.cursor.return_value.__enter__.return_value = mock_cursor
    
    try:
        execute_sql_select(script_data.dbh, "SELECT * FROM test")
        assert False, "Should have raised exception"
    except Exception as e:
        assert "SQL error" in str(e)


def test_initialize_function(script_data):
    """Test the initialize function creates proper ScriptData"""
    with patch('delivery_method_update.dna_db_connect') as mock_db_connect, \
         patch('delivery_method_update.get_config') as mock_get_config, \
         patch('delivery_method_update.get_email_template') as mock_get_email_template:
        
        mock_db_connect.return_value = MagicMock()
        mock_get_config.return_value = {"test": "config"}
        mock_get_email_template.return_value = MagicMock()
        
        result = initialize(script_data.apwx)
        
        assert result.apwx == script_data.apwx
        assert result.dbh is not None
        assert result.config == {"test": "config"}
        assert result.email_template is not None
        mock_db_connect.assert_called_once_with(script_data.apwx)
        mock_get_config.assert_called_once_with(script_data.apwx)
        mock_get_email_template.assert_called_once()