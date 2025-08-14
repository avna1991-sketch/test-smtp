import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from email.message import EmailMessage

from production_scripts_statement_delivery_method_update.delivery_method_update import (
    run,
    initialize,
    fetch_records,
    process_records,
    update_stdl_userfield,
    write_report_file,
    write_report,
    send_notification_email,
    send_email,
    generate_email_message,
    generate_email_content,
    send_smtp_request,
    is_local_environment,
    send_email_enabled,
    get_config,
    get_email_template,
    execute_sql_select,
    dna_db_connect,
    AppWorxEnum,
    ScriptData,
)


class TestDeliveryMethodUpdate:
    """Test class for delivery method update functionality"""

    def test_initialize(self, script_data):
        """Test the initialize function"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.dna_db_connect') as mock_db_connect, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.get_config') as mock_get_config, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.get_email_template') as mock_get_email_template:
            
            mock_db_connect.return_value = MagicMock()
            mock_get_config.return_value = {"test": "config"}
            mock_get_email_template.return_value = MagicMock()
            
            result = initialize(script_data.apwx)
            
            assert isinstance(result, ScriptData)
            assert result.apwx == script_data.apwx
            mock_db_connect.assert_called_once_with(script_data.apwx)
            mock_get_config.assert_called_once_with(script_data.apwx)
            mock_get_email_template.assert_called_once()

    def test_dna_db_connect_report_mode(self, script_data_report_only):
        """Test database connection in report-only mode"""
        with patch.object(script_data_report_only.apwx, 'db_connect') as mock_db_connect:
            mock_conn = MagicMock()
            mock_db_connect.return_value = mock_conn
            
            result = dna_db_connect(script_data_report_only.apwx)
            
            assert result == mock_conn
            assert mock_conn.autocommit is False
            mock_db_connect.assert_called_once_with(autocommit=False)

    def test_dna_db_connect_normal_mode(self, script_data):
        """Test database connection in normal mode"""
        with patch.object(script_data.apwx, 'db_connect') as mock_db_connect:
            mock_conn = MagicMock()
            mock_db_connect.return_value = mock_conn
            
            result = dna_db_connect(script_data.apwx)
            
            assert result == mock_conn
            assert mock_conn.autocommit is True
            mock_db_connect.assert_called_once_with(autocommit=False)

    def test_fetch_records_with_run_date(self, script_data, sample_pers_records, sample_org_records):
        """Test fetching records with specific run date"""
        all_records = sample_pers_records + sample_org_records
        
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.execute_sql_select') as mock_execute:
            mock_execute.return_value = all_records
            
            pers_records, org_records = fetch_records(script_data)
            
            assert len(pers_records) == 2
            assert len(org_records) == 2
            assert all(r['ENTITY_TYPE'] == 'pers' for r in pers_records)
            assert all(r['ENTITY_TYPE'] == 'org' for r in org_records)
            mock_execute.assert_called_once()

    def test_fetch_records_with_full_cleanup(self, script_data_full_cleanup, sample_pers_records, sample_org_records):
        """Test fetching records with full cleanup mode"""
        all_records = sample_pers_records + sample_org_records
        
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.execute_sql_select') as mock_execute:
            mock_execute.return_value = all_records
            
            pers_records, org_records = fetch_records(script_data_full_cleanup)
            
            assert len(pers_records) == 2
            assert len(org_records) == 2
            mock_execute.assert_called_once()

    def test_fetch_records_parameter_validation_both_provided(self, script_data):
        """Test parameter validation when both RUN_DATE and FULL_CLEANUP_YN are provided"""
        # Modify script_data to have both parameters
        script_data.apwx.args.RUN_DATE = "01-15-2024"
        script_data.apwx.args.FULL_CLEANUP_YN = "Y"
        
        with pytest.raises(Exception) as exc_info:
            fetch_records(script_data)
        
        assert "mutually exclusive" in str(exc_info.value)

    def test_fetch_records_parameter_validation_neither_provided(self, script_data):
        """Test parameter validation when neither parameter is provided"""
        script_data.apwx.args.RUN_DATE = None
        script_data.apwx.args.FULL_CLEANUP_YN = "N"
        
        with pytest.raises(Exception) as exc_info:
            fetch_records(script_data)
        
        assert "no RUN_DATE parameter provided" in str(exc_info.value)

    def test_process_records_success(self, script_data, sample_pers_records, sample_org_records):
        """Test successful processing of records"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.update_stdl_userfield') as mock_update, \
             patch('pathlib.Path.exists') as mock_exists:
            
            mock_exists.return_value = False
            mock_update.side_effect = [
                ([('success1',)], [('fail1',)]),  # person records
                ([('success2',)], [('fail2',)])   # org records
            ]
            
            successes, fails = process_records(script_data, sample_pers_records, sample_org_records)
            
            assert len(successes) == 2
            assert len(fails) == 2
            assert mock_update.call_count == 2

    def test_process_records_file_exists_error(self, script_data, sample_pers_records, sample_org_records):
        """Test error when output file already exists"""
        with patch('pathlib.Path.exists') as mock_exists:
            mock_exists.return_value = True
            
            with pytest.raises(FileExistsError):
                process_records(script_data, sample_pers_records, sample_org_records)

    def test_update_stdl_userfield_empty_records(self, script_data):
        """Test update function with empty records"""
        successes, fails = update_stdl_userfield(script_data, [], 'persuserfield', 'persnbr')
        
        assert successes == []
        assert fails == []

    def test_update_stdl_userfield_with_records(self, script_data, sample_pers_records):
        """Test update function with person records"""
        mock_cursor = MagicMock()
        mock_cursor.getbatcherrors.return_value = []
        mock_cursor.rowcount = 2
        script_data.dbh.cursor.return_value.__enter__.return_value = mock_cursor
        
        successes, fails = update_stdl_userfield(script_data, sample_pers_records, 'persuserfield', 'persnbr')
        
        assert len(successes) == 2
        assert len(fails) == 0
        mock_cursor.executemany.assert_called_once()

    def test_update_stdl_userfield_with_batch_errors(self, script_data, sample_pers_records):
        """Test update function with batch errors"""
        mock_cursor = MagicMock()
        mock_error = MagicMock()
        mock_error.offset = 0
        mock_error.message = "Test error"
        mock_cursor.getbatcherrors.return_value = [mock_error]
        mock_cursor.rowcount = 1
        script_data.dbh.cursor.return_value.__enter__.return_value = mock_cursor
        
        successes, fails = update_stdl_userfield(script_data, sample_pers_records, 'persuserfield', 'persnbr')
        
        assert len(fails) > 0
        mock_cursor.executemany.assert_called_once()

    def test_write_report_file_with_successes_and_fails(self, script_data, sample_success_records, sample_fail_records):
        """Test writing report file with both successes and failures"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.write_report') as mock_write_report:
            write_report_file(script_data, sample_success_records, sample_fail_records)
            
            assert mock_write_report.call_count == 2
            # First call should be with write mode 'w' for successes
            mock_write_report.assert_any_call(
                Path(script_data.apwx.args.OUTPUT_FILE_PATH) / script_data.apwx.args.OUTPUT_FILE_NAME,
                sample_success_records,
                write_mode='w'
            )
            # Second call should be with append mode 'a+' for failures
            mock_write_report.assert_any_call(
                Path(script_data.apwx.args.OUTPUT_FILE_PATH) / script_data.apwx.args.OUTPUT_FILE_NAME,
                sample_fail_records,
                write_mode='a+'
            )

    def test_write_report_file_only_successes(self, script_data, sample_success_records):
        """Test writing report file with only successes"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.write_report') as mock_write_report:
            write_report_file(script_data, sample_success_records, [])
            
            assert mock_write_report.call_count == 1
            mock_write_report.assert_called_once_with(
                Path(script_data.apwx.args.OUTPUT_FILE_PATH) / script_data.apwx.args.OUTPUT_FILE_NAME,
                sample_success_records,
                write_mode='w'
            )

    def test_write_report(self, sample_success_records):
        """Test the write_report function"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            temp_path = temp_file.name
        
        try:
            result = write_report(temp_path, sample_success_records, 'w')
            assert result is True
            
            # Verify file was written correctly
            with open(temp_path, 'r') as f:
                lines = f.readlines()
                assert len(lines) == 4  # Header + 3 records
                assert 'ENTITY_NBR,ACCTNBR,ENTITY_TYPE,CLOSE_DATE,RESULT' in lines[0]
        finally:
            os.unlink(temp_path)

    def test_send_notification_email_with_fails(self, script_data, sample_fail_records):
        """Test sending notification email when there are failures"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.send_email') as mock_send_email:
            mock_send_email.return_value = (True, "Email Sent")
            
            send_notification_email(script_data, sample_fail_records)
            
            mock_send_email.assert_called_once()

    def test_send_notification_email_no_fails(self, script_data):
        """Test not sending notification email when there are no failures"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.send_email') as mock_send_email:
            send_notification_email(script_data, [])
            
            mock_send_email.assert_not_called()

    def test_send_email_success(self, script_data):
        """Test successful email sending"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.generate_email_content') as mock_content, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.generate_email_message') as mock_message, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.is_local_environment') as mock_local, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.send_email_enabled') as mock_enabled, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.send_smtp_request') as mock_smtp:
            
            mock_content.return_value = "Test email content"
            mock_message.return_value = MagicMock()
            mock_local.return_value = False
            mock_enabled.return_value = True
            
            result, message = send_email(script_data, ["test@example.com"])
            
            assert result is True
            assert message == "Email Sent"
            mock_smtp.assert_called_once()

    def test_send_email_disabled_local_env(self, script_data):
        """Test email sending disabled in local environment"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.is_local_environment') as mock_local:
            mock_local.return_value = True
            
            result, message = send_email(script_data, ["test@example.com"])
            
            assert result is False
            assert message == "Email Send Disabled"

    def test_send_email_no_recipients(self, script_data):
        """Test email sending with no recipients"""
        result, message = send_email(script_data, [])
        
        assert result is False
        assert message == "No email recipients"

    def test_generate_email_message(self):
        """Test email message generation"""
        from_addr = "from@example.com"
        to_addr = "to@example.com"
        content = "<html><body>Test content</body></html>"
        
        message = generate_email_message(from_addr, to_addr, content)
        
        assert isinstance(message, EmailMessage)
        assert message["From"] == f"First Tech Federal Credit Union <{from_addr}>"
        assert message["To"] == to_addr
        assert message["Subject"] == "Statement Delivery Method Update Alert"

    def test_generate_email_content(self, script_data):
        """Test email content generation"""
        mock_template = MagicMock()
        mock_template.render.return_value = "Rendered content"
        script_data.email_template = mock_template
        
        content = generate_email_content(script_data)
        
        assert content == "Rendered content"
        mock_template.render.assert_called_once()

    def test_send_smtp_request(self, script_data):
        """Test SMTP request sending"""
        with patch('smtplib.SMTP') as mock_smtp_class:
            mock_server = MagicMock()
            mock_smtp_class.return_value.__enter__.return_value = mock_server
            
            from_addr = "from@example.com"
            to_addr = "to@example.com"
            message = MagicMock()
            message.as_string.return_value = "Email content"
            
            send_smtp_request(script_data.apwx, from_addr, to_addr, message)
            
            mock_server.connect.assert_called_once()
            mock_server.starttls.assert_called_once()
            mock_server.login.assert_called_once()
            mock_server.sendmail.assert_called_once_with(from_addr, to_addr, "Email content")

    def test_is_local_environment_with_aw_home(self):
        """Test local environment detection with AW_HOME set"""
        with patch.dict(os.environ, {'AW_HOME': '/some/path'}):
            assert is_local_environment() is False

    def test_is_local_environment_without_aw_home(self):
        """Test local environment detection without AW_HOME"""
        with patch.dict(os.environ, {}, clear=True):
            assert is_local_environment() is True

    def test_send_email_enabled_true(self, script_data):
        """Test email enabled check when enabled"""
        script_data.apwx.args.SEND_EMAIL_YN = "Y"
        assert send_email_enabled(script_data.apwx) is True

    def test_send_email_enabled_false(self, script_data):
        """Test email enabled check when disabled"""
        script_data.apwx.args.SEND_EMAIL_YN = "N"
        assert send_email_enabled(script_data.apwx) is False

    def test_get_config(self, script_data):
        """Test configuration loading"""
        mock_config = {"test": "configuration"}
        
        with patch('builtins.open', mock_open(read_data='test: configuration')), \
             patch('yaml.safe_load') as mock_yaml_load:
            mock_yaml_load.return_value = mock_config
            
            config = get_config(script_data.apwx)
            
            assert config == mock_config
            mock_yaml_load.assert_called_once()

    def test_get_email_template(self):
        """Test email template loading"""
        mock_config = {
            "template_directory": "templates",
            "template_file": "email_template.html"
        }
        
        with patch('os.path.dirname') as mock_dirname, \
             patch('os.path.abspath') as mock_abspath, \
             patch('os.path.join') as mock_join, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.FileSystemLoader') as mock_loader, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.Environment') as mock_env:
            
            mock_dirname.return_value = "/test/dir"
            mock_abspath.return_value = "/test/dir/script.py"
            mock_join.return_value = "/test/dir/templates"
            
            mock_env_instance = MagicMock()
            mock_template = MagicMock()
            mock_env_instance.get_template.return_value = mock_template
            mock_env.return_value = mock_env_instance
            
            result = get_email_template(mock_config)
            
            assert result == mock_template
            mock_env_instance.get_template.assert_called_once_with("email_template.html")

    def test_execute_sql_select_success(self, script_data):
        """Test successful SQL execution"""
        mock_cursor = MagicMock()
        mock_cursor.description = [('COL1',), ('COL2',)]
        mock_cursor.fetchall.return_value = [('val1', 'val2'), ('val3', 'val4')]
        script_data.dbh.cursor.return_value.__enter__.return_value = mock_cursor
        
        result = execute_sql_select(script_data.dbh, "SELECT * FROM test")
        
        assert len(result) == 2
        assert result[0] == {'COL1': 'val1', 'COL2': 'val2'}
        assert result[1] == {'COL1': 'val3', 'COL2': 'val4'}
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", None)

    def test_execute_sql_select_with_params(self, script_data):
        """Test SQL execution with parameters"""
        mock_cursor = MagicMock()
        mock_cursor.description = [('COL1',)]
        mock_cursor.fetchall.return_value = [('val1',)]
        script_data.dbh.cursor.return_value.__enter__.return_value = mock_cursor
        
        params = {'param1': 'value1'}
        result = execute_sql_select(script_data.dbh, "SELECT * FROM test WHERE col = :param1", params)
        
        assert len(result) == 1
        assert result[0] == {'COL1': 'val1'}
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test WHERE col = :param1", params)

    def test_execute_sql_select_exception(self, script_data):
        """Test SQL execution with exception"""
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Database error")
        script_data.dbh.cursor.return_value.__enter__.return_value = mock_cursor
        
        with pytest.raises(Exception) as exc_info:
            execute_sql_select(script_data.dbh, "SELECT * FROM test")
        
        assert "SQL error" in str(exc_info.value)

    def test_run_function_integration(self, script_data):
        """Test the main run function integration"""
        with patch('production_scripts_statement_delivery_method_update.delivery_method_update.initialize') as mock_init, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.fetch_records') as mock_fetch, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.process_records') as mock_process, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.write_report_file') as mock_write, \
             patch('production_scripts_statement_delivery_method_update.delivery_method_update.send_notification_email') as mock_email:
            
            mock_init.return_value = script_data
            mock_fetch.return_value = ([], [])  # empty person and org records
            mock_process.return_value = ([], [])  # empty successes and failures
            
            result = run(script_data.apwx)
            
            assert result is True
            mock_init.assert_called_once_with(script_data.apwx)
            mock_fetch.assert_called_once_with(script_data)
            mock_process.assert_called_once()
            mock_write.assert_called_once()
            mock_email.assert_called_once()
            script_data.dbh.close.assert_called_once()