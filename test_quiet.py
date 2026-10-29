import contextlib
import io
import logging
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import connect_to_fargate as app


class QuietTests(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        app_dir = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(patch.object(app, 'get_app_dir', return_value=app_dir))
        self.logger = logging.Logger('quiet-test')
        self.stack.enter_context(patch.object(app.logging, 'getLogger', return_value=self.logger))
        self.stack.callback(self.close_handlers)
        self.stdout = self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.stderr = self.stack.enter_context(contextlib.redirect_stderr(io.StringIO()))

    def close_handlers(self):
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            handler.close()

    def test_default_keeps_path_notifications_and_normal_messages(self):
        logger, initial = app.setLogger()
        logger.info('通常メッセージ')
        updated = app.update_logfile_path(logger, initial, 'cluster', 'svc', 'app')
        self.assertIn('ログファイル: ' + initial, self.stdout.getvalue())
        self.assertIn('ログファイル更新: ' + updated, self.stdout.getvalue())
        self.assertIn('通常メッセージ', self.stderr.getvalue())
        self.assertIn('通常メッセージ', Path(updated).read_text())

    def test_quiet_hides_info_and_paths_but_preserves_logs_and_errors(self):
        logger, initial = app.setLogger(quiet=True)
        logger.info('通常メッセージ')
        updated = app.update_logfile_path(logger, initial, 'cluster', 'svc', 'app')
        logger.info('更新後メッセージ')
        logger.warning('警告メッセージ')
        logger.error('エラーメッセージ')
        self.assertEqual(self.stdout.getvalue(), '')
        self.assertEqual(self.stderr.getvalue(), '警告メッセージ\nエラーメッセージ\n')
        contents = Path(updated).read_text()
        for message in ('通常メッセージ', '更新後メッセージ', '警告メッセージ', 'エラーメッセージ'):
            self.assertIn(message, contents)
        self.assertFalse(Path(initial).exists())

    def test_main_applies_both_quiet_aliases(self):
        for flag in ('-q', '--quiet'):
            with self.subTest(flag=flag):
                def flow(args, logger, logfile):
                    self.assertTrue(args.quiet)
                    logger.info('処理を開始します')
                    print('コンテナの出力')
                    return logfile

                with patch.object(app, 'run_main_flow', side_effect=flow):
                    app.main([flag])
                self.close_handlers()
        self.assertEqual(self.stdout.getvalue(), 'コンテナの出力\nコンテナの出力\n')
        self.assertEqual(self.stderr.getvalue(), '')

    def test_main_error_still_visible_in_quiet_mode(self):
        with patch.object(app, 'run_main_flow', side_effect=RuntimeError('接続失敗')):
            with self.assertRaises(SystemExit) as error:
                app.main(['--quiet'])
        self.assertEqual(error.exception.code, 255)
        self.assertEqual(self.stdout.getvalue(), '')
        self.assertIn('接続失敗', self.stderr.getvalue())

    def test_traceback_visibility_and_file_preservation(self):
        for quiet in (False, True):
            for show_traceback in (False, True):
                with self.subTest(quiet=quiet, show_traceback=show_traceback):
                    flags = (['--quiet'] if quiet else []) + (['--traceback'] if show_traceback else [])
                    self.stderr.truncate(0)
                    self.stderr.seek(0)
                    with patch.object(app, 'run_main_flow', side_effect=RuntimeError('接続失敗')):
                        with self.assertRaises(SystemExit) as error:
                            app.main(flags)
                    self.assertEqual(error.exception.code, 255)
                    output = self.stderr.getvalue()
                    self.assertIn('処理を終了します\nエラー詳細: 接続失敗', output)
                    self.assertEqual('Traceback (most recent call last):' in output, show_traceback)
                    self.assertEqual('RuntimeError: 接続失敗' in output, show_traceback)
                    contents = Path(self.logger.logfile_name).read_text()
                    self.assertIn('Traceback (most recent call last):', contents)
                    self.assertIn('RuntimeError: 接続失敗', contents)
                    self.close_handlers()

    def test_traceback_option_when_logger_initialization_fails(self):
        for flags in ([], ['--traceback'], ['--quiet'], ['--quiet', '--traceback']):
            with self.subTest(flags=flags):
                self.stderr.truncate(0)
                self.stderr.seek(0)
                with patch.object(app, 'setLogger', side_effect=OSError('ログ作成失敗')):
                    with self.assertRaises(SystemExit) as error:
                        app.main(flags)
                self.assertEqual(error.exception.code, 255)
                self.assertIn('エラー詳細: ログ作成失敗', self.stderr.getvalue())
                self.assertEqual('Traceback (most recent call last):' in self.stderr.getvalue(), '--traceback' in flags)

    def test_console_formatting_does_not_strip_file_exception_information(self):
        logger, logfile = app.setLogger()
        # 画面用ハンドラが先でもファイルへのTraceback保存を維持する。
        logger.handlers.reverse()
        try:
            raise RuntimeError('例外情報の保持')
        except RuntimeError:
            logger.error('エラー', exc_info=True)
        self.assertEqual(self.stderr.getvalue(), 'エラー\n')
        self.assertIn('RuntimeError: 例外情報の保持', Path(logfile).read_text())

    def test_sso_recovery_is_preserved(self):
        with patch.object(app, 'run_main_flow', side_effect=[RuntimeError('SSO失効'), None]) as flow, \
                patch.object(app, 'is_invalid_sso_session_error', return_value=True) as check, \
                patch.object(app, 'recover_invalid_sso_session') as recover:
            app.main(['-p', 'profile', '--quiet'])
        self.assertEqual(flow.call_count, 2)
        self.assertIn('Traceback (most recent call last):', check.call_args.args[0])
        recover.assert_called_once()
        self.assertIn('再ログイン後に1回だけ再試行', self.stderr.getvalue())
        self.assertNotIn('Traceback (most recent call last):', self.stderr.getvalue())

    def test_argument_errors_keep_usage_without_traceback(self):
        for flags in (['--unknown'], ['--traceback', '--unknown']):
            with self.subTest(flags=flags):
                self.stderr.truncate(0)
                self.stderr.seek(0)
                with self.assertRaises(SystemExit) as error:
                    app.main(flags)
                self.assertEqual(error.exception.code, 2)
                self.assertIn('usage:', self.stderr.getvalue())
                self.assertNotIn('Traceback (most recent call last):', self.stderr.getvalue())

    def test_quiet_keeps_connection_prompt_and_session_output(self):
        logger, logfile = app.setLogger(quiet=True)

        def session(*args, **kwargs):
            print('Starting session', file=kwargs['stdout'])
            print('コンテナの出力', file=kwargs['stdout'])
            return subprocess.CompletedProcess(args[0], 0)

        with patch.object(app, 'selected_answer', return_value='yes') as prompt, \
                patch.object(app, 'get_aws_cli_path', return_value='/usr/local/bin/aws'), \
                patch.object(app.signal, 'signal'), \
                patch.object(app.subprocess, 'run', side_effect=session) as run:
            app.ecsExecute(logger, 'cluster', 'svc', 'task', 'app', 'echo test', logfile, False)
        prompt.assert_called_once_with(['yes', 'no'], 'こちらに接続してよろしいですか')
        self.assertEqual(self.stdout.getvalue(), 'Starting session\nコンテナの出力\n')
        self.assertEqual(self.stderr.getvalue(), '')
        self.assertIn('tee -a', run.call_args.args[0][2])
        self.assertIn('Fargateにログインします', Path(logfile).read_text())

    def test_help_remains_visible_with_quiet(self):
        with self.assertRaises(SystemExit) as error:
            app.main(['--quiet', '--help'])
        self.assertEqual(error.exception.code, 0)
        self.assertIn('--quiet', self.stdout.getvalue())
        self.assertIn('--traceback', self.stdout.getvalue())
        self.assertEqual(self.stderr.getvalue(), '')


if __name__ == '__main__':
    unittest.main()
