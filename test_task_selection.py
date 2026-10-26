import datetime
import unittest
from unittest.mock import Mock, patch

import connect_to_fargate as app


def task(task_id, seconds=0, group='service:svc', **overrides):
    result = {
        'taskArn': 'arn:aws:ecs:region:account:task/cluster/' + task_id,
        'group': group,
        'desiredStatus': 'RUNNING',
        'lastStatus': 'RUNNING',
        'startedAt': datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        + datetime.timedelta(seconds=seconds),
    }
    result.update(overrides)
    return result


class TaskSelectionTests(unittest.TestCase):
    def setUp(self):
        self.logger = Mock()

    def select(self, tasks, rule='prompt'):
        with patch.object(app, 'get_task_candidates', return_value=tasks), \
                patch.object(app, 'selected_answer', return_value='b') as prompt:
            result = app.setTask(self.logger, 'cluster', 'svc', rule)
        return result, prompt

    def test_single_candidate_skips_prompt_for_every_rule(self):
        for rule in ('prompt', 'latest', 'oldest'):
            with self.subTest(rule=rule):
                result, prompt = self.select([task('a')], rule)
                self.assertEqual(result, 'a')
                prompt.assert_not_called()
        self.assertIn('候補件数=1', self.logger.info.call_args_list[0].args[0])

    def test_multiple_candidates_prompt_by_default(self):
        result, prompt = self.select([task('a'), task('b')])
        self.assertEqual(result, 'b')
        self.assertEqual(prompt.call_args.args[0], ['a', 'b'])

    def test_time_rules_ignore_api_order_and_task_id_order(self):
        tasks = [task('a', 10), task('b', 0), task('c', 20)]
        for rule, expected in (('latest', 'c'), ('oldest', 'b')):
            with self.subTest(rule=rule):
                result, prompt = self.select(tasks, rule)
                self.assertEqual(result, expected)
                prompt.assert_not_called()

    def test_same_time_uses_task_id_ascending(self):
        for rule in ('latest', 'oldest'):
            with self.subTest(rule=rule):
                self.assertEqual(self.select([task('z'), task('a')], rule)[0], 'a')

    def test_no_candidates_fails_without_prompt(self):
        with patch.object(app, 'get_task_candidates', return_value=[]), \
                patch.object(app, 'selected_answer') as prompt:
            with self.assertRaisesRegex(Exception, '稼働中のタスクが存在しません'):
                app.setTask(self.logger, 'cluster', 'svc')
            prompt.assert_not_called()

    def test_missing_start_time_prevents_time_selection(self):
        for rule in ('latest', 'oldest'):
            with self.subTest(rule=rule):
                with self.assertRaisesRegex(Exception, '起動日時が不明'):
                    self.select([task('a'), task('b', startedAt=None)], rule)

    def test_stopped_selected_task_does_not_fall_back(self):
        with patch.object(app, 'get_task_candidates', side_effect=[
            [task('a', 10), task('b')], [task('b')],
        ]) as candidates, patch.object(app, 'selected_answer') as prompt:
            with self.assertRaisesRegex(Exception, '別タスクへの切り替えは行いません'):
                app.setTask(self.logger, 'cluster', 'svc', 'latest')
            self.assertEqual(candidates.call_count, 2)
            prompt.assert_not_called()


class TaskCandidateTests(unittest.TestCase):
    @patch.object(app, 'get_ecs_client')
    def test_pagination_and_describe_batches(self, get_client):
        tasks = [task(str(i)) for i in range(101)]
        ecs = get_client.return_value
        ecs.list_tasks.side_effect = [
            {'taskArns': [t['taskArn'] for t in tasks[:100]], 'nextToken': 'page2'},
            {'taskArns': [tasks[0]['taskArn'], tasks[100]['taskArn']]},
        ]
        ecs.describe_tasks.side_effect = [{'tasks': tasks[:100]}, {'tasks': tasks[100:]}]
        result = app.get_task_candidates('cluster', 'svc')
        self.assertEqual(len(result), 101)
        self.assertEqual(ecs.list_tasks.call_args_list[1].kwargs['nextToken'], 'page2')
        self.assertEqual(ecs.list_tasks.call_args_list[0].kwargs['serviceName'], 'svc')
        self.assertEqual([len(call.kwargs['tasks']) for call in ecs.describe_tasks.call_args_list], [100, 1])

    @patch.object(app, 'get_ecs_client')
    def test_membership_and_actual_status_filtering(self, get_client):
        tasks = [task('service'), task('standalone', group='family:batch'),
                 task('pending', lastStatus='PENDING'), task('stopping', desiredStatus='STOPPED'),
                 task('other', group='service:other')]
        ecs = get_client.return_value
        ecs.list_tasks.return_value = {'taskArns': [t['taskArn'] for t in tasks]}
        ecs.describe_tasks.return_value = {'tasks': tasks}
        self.assertEqual(app.get_task_candidates('cluster', 'svc'), [tasks[0]])
        self.assertEqual(app.get_task_candidates('cluster', None), [tasks[1]])
        self.assertNotIn('serviceName', ecs.list_tasks.call_args.kwargs)
        self.assertFalse(app.checkTask('cluster', None, 'service'))
        self.assertTrue(app.checkTask('cluster', None, 'standalone'))

    @patch.object(app, 'get_ecs_client')
    def test_empty_list_does_not_describe(self, get_client):
        ecs = get_client.return_value
        ecs.list_tasks.return_value = {'taskArns': []}
        self.assertEqual(app.get_task_candidates('cluster', 'svc'), [])
        ecs.describe_tasks.assert_not_called()

    @patch.object(app, 'get_ecs_client')
    def test_describe_failure_does_not_select_from_partial_results(self, get_client):
        ecs = get_client.return_value
        ecs.list_tasks.return_value = {'taskArns': [task('a')['taskArn']]}
        ecs.describe_tasks.return_value = {'tasks': [], 'failures': [{'reason': 'MISSING'}]}
        with self.assertRaisesRegex(Exception, 'タスク情報の取得に失敗'):
            app.get_task_candidates('cluster', 'svc')


class TaskArgumentTests(unittest.TestCase):
    def test_default_and_all_rules(self):
        parser = app.build_parser()
        self.assertEqual(parser.parse_args([]).task_selection, 'prompt')
        for rule in ('prompt', 'latest', 'oldest'):
            self.assertEqual(parser.parse_args(['--task-selection', rule]).task_selection, rule)
        self.assertEqual(parser.parse_args(['--task', 'abc']).task, 'abc')

    def test_conflicting_or_unknown_rules_fail(self):
        for arguments in (
            ['--task', 'abc', '--task-selection', 'latest'],
            ['--task', 'abc', '--task-selection', 'prompt'],
            ['--task-selection', 'first'],
        ):
            with self.subTest(arguments=arguments), patch('sys.stderr'), self.assertRaises(SystemExit) as error:
                app.build_parser().parse_args(arguments)
            self.assertEqual(error.exception.code, 2)


class TaskFlowTests(unittest.TestCase):
    def test_main_flow_passes_rule_and_preserves_explicit_task(self):
        for task_args, expected_task, expected_rule in (
            ([], 'auto-task', 'prompt'),
            (['--task-selection', 'latest'], 'auto-task', 'latest'),
            (['--task-selection', 'oldest'], 'auto-task', 'oldest'),
            (['--task', 'explicit-task'], 'explicit-task', None),
        ):
            with self.subTest(task_args=task_args):
                args = app.build_parser().parse_args([
                    '-p', 'profile', '-c', 'cluster', '-s', 'svc', '-t', 'app',
                    '--cmd', 'php artisan --version', '-f', *task_args,
                ])
                logger = Mock()
                select_task = Mock(return_value='auto-task')
                execute = Mock()
                with patch.multiple(
                    app, resolve_aws_profile=Mock(return_value='profile'),
                    ensure_aws_sso_login=Mock(), checkCluster=Mock(return_value=True),
                    checkService=Mock(return_value=True), checkTask=Mock(return_value=True),
                    checkContainer=Mock(return_value=True), setTask=select_task,
                    update_logfile_path=Mock(return_value='updated.log'), ecsExecute=execute,
                ):
                    self.assertEqual(app.run_main_flow(args, logger, 'initial.log'), 'updated.log')
                if expected_rule is None:
                    select_task.assert_not_called()
                else:
                    select_task.assert_called_once_with(logger, 'cluster', 'svc', expected_rule)
                execute.assert_called_once_with(
                    logger, 'cluster', 'svc', expected_task, 'app',
                    'php artisan --version', 'updated.log', True,
                )


if __name__ == '__main__':
    unittest.main()
