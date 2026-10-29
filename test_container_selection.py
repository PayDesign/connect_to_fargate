import unittest
from unittest.mock import Mock, patch

import connect_to_fargate as app


class ContainerSelectionTests(unittest.TestCase):
    def select(self, names, selection='auto', fallback='error'):
        with patch.object(app, 'get_container_candidates', return_value=names), \
                patch.object(app, 'selected_answer', return_value=names[-1]) as prompt:
            result = app.setContainer(Mock(), 'cluster', 'task', selection, fallback)
        return result, prompt

    def test_priority_is_independent_of_api_order(self):
        priority = [
            'app', 'web', 'scheduler', 'queue', 'email', 'webhook', 'worker', 'docs', 'queue-redis',
            'api', 'nginx', 'seven-worker', 'queue-email',
        ]
        for index, expected in enumerate(priority):
            names = list(reversed(priority[index:]))
            with self.subTest(names=names):
                result, prompt = self.select(names)
                self.assertEqual(result, expected)
                prompt.assert_not_called()

    def test_no_exact_match_errors_by_default_and_with_explicit_error(self):
        for extra in ([], ['error']):
            with self.subTest(extra=extra), \
                    patch.object(app, 'get_container_candidates', return_value=['my-app', 'App']), \
                    patch.object(app, 'selected_answer') as prompt:
                with self.assertRaisesRegex(Exception, '優先候補.*存在しません'):
                    app.setContainer(Mock(), 'cluster', 'task', 'auto', *extra)
                prompt.assert_not_called()

    def test_fallback_prompts_only_when_no_priority_candidate_exists(self):
        result, prompt = self.select(['proxy', 'sidecar'], fallback='prompt')
        self.assertEqual(result, 'sidecar')
        self.assertEqual(prompt.call_args.args[0], ['proxy', 'sidecar'])
        result, prompt = self.select(['nginx', 'app'], fallback='prompt')
        self.assertEqual(result, 'app')
        prompt.assert_not_called()

    def test_default_keeps_prompt_even_with_one_candidate(self):
        result, prompt = self.select(['app'], selection='prompt')
        self.assertEqual(result, 'app')
        prompt.assert_called_once()

    def test_empty_candidates_always_error(self):
        for selection, fallback in (('prompt', 'error'), ('auto', 'error'), ('auto', 'prompt')):
            with self.subTest(selection=selection, fallback=fallback), \
                    patch.object(app, 'get_container_candidates', return_value=[]), \
                    patch.object(app, 'selected_answer') as prompt:
                with self.assertRaisesRegex(Exception, 'コンテナが存在しません'):
                    app.setContainer(Mock(), 'cluster', 'task', selection, fallback)
                prompt.assert_not_called()

    def test_disappearing_container_does_not_fall_back(self):
        with patch.object(app, 'get_container_candidates', side_effect=[['app', 'web'], ['web']]), \
                patch.object(app, 'selected_answer') as prompt:
            with self.assertRaisesRegex(Exception, '切り替えは行いません'):
                app.setContainer(Mock(), 'cluster', 'task', 'auto', 'prompt')
            prompt.assert_not_called()


class ContainerCandidateTests(unittest.TestCase):
    @patch.object(app, 'get_ecs_client')
    def test_reads_actual_containers_and_ignores_overrides(self, client):
        ecs = client.return_value
        ecs.describe_tasks.return_value = {'tasks': [{
            'containers': [{'name': 'app'}, {'name': 'web'}],
            'overrides': {'containerOverrides': [{'name': 'override-only'}]},
        }]}
        self.assertEqual(app.get_container_candidates('cluster', 'task'), ['app', 'web'])
        self.assertTrue(app.checkContainer('cluster', 'task', 'app'))
        self.assertFalse(app.checkContainer('cluster', 'task', 'override-only'))
        ecs.describe_tasks.assert_called_with(cluster='cluster', tasks=['task'])
        ecs.describe_tasks.return_value['tasks'][0]['overrides']['containerOverrides'] = []
        self.assertTrue(app.checkContainer('cluster', 'task', 'app'))

    @patch.object(app, 'get_ecs_client')
    def test_api_failure_does_not_use_partial_results(self, client):
        client.return_value.describe_tasks.return_value = {
            'tasks': [{'containers': [{'name': 'app'}]}], 'failures': [{'reason': 'MISSING'}],
        }
        with self.assertRaisesRegex(Exception, 'コンテナ情報の取得に失敗'):
            app.get_container_candidates('cluster', 'task')

    @patch.object(app, 'get_ecs_client')
    def test_missing_task_returns_no_candidates(self, client):
        client.return_value.describe_tasks.return_value = {'tasks': []}
        self.assertEqual(app.get_container_candidates('cluster', 'task'), [])


class ContainerArgumentTests(unittest.TestCase):
    def test_defaults_and_supported_values(self):
        args = app.build_parser().parse_args([])
        self.assertEqual((args.container_selection, args.container_selection_fallback), ('prompt', 'error'))
        for fallback in ('error', 'prompt'):
            args = app.build_parser().parse_args([
                '--task', 'explicit', '--container-selection=auto',
                '--container-selection-fallback=' + fallback,
            ])
            self.assertEqual(args.container_selection_fallback, fallback)
            self.assertEqual(args.task, 'explicit')

    def test_invalid_combinations_fail(self):
        for arguments in (
            ['--container', 'app', '--container-selection', 'auto'],
            ['--container', 'app', '--container-selection', 'prompt'],
            ['--container-selection-fallback', 'error'],
            ['--container-selection', 'prompt', '--container-selection-fallback', 'prompt'],
            ['--container', 'app', '--container-selection-fallback', 'error'],
            ['--container-selection', 'unknown'],
            ['--container-selection', 'auto', '--container-selection-fallback', 'selection'],
        ):
            with self.subTest(arguments=arguments), patch('sys.stderr'), self.assertRaises(SystemExit) as error:
                app.build_parser().parse_args(arguments)
            self.assertEqual(error.exception.code, 2)


class ContainerFlowTests(unittest.TestCase):
    def test_explicit_task_is_preserved_and_rules_are_passed(self):
        for flags, expected_selection in (
            ([], ('prompt', 'error')),
            (['--container-selection', 'auto'], ('auto', 'error')),
            (['--container-selection', 'auto', '--container-selection-fallback', 'prompt'], ('auto', 'prompt')),
            (['--container', 'web'], None),
        ):
            with self.subTest(flags=flags):
                args = app.build_parser().parse_args([
                    '-p', 'profile', '-c', 'cluster', '-s', 'svc', '--task', 'explicit', *flags,
                ])
                logger = Mock()
                select_task = Mock()
                select_container = Mock(return_value='app')
                execute = Mock()
                with patch.multiple(
                    app, resolve_aws_profile=Mock(return_value='profile'), ensure_aws_sso_login=Mock(),
                    checkCluster=Mock(return_value=True), checkService=Mock(return_value=True),
                    checkTask=Mock(return_value=True), checkContainer=Mock(return_value=True),
                    setTask=select_task, setContainer=select_container, ecsExecute=execute,
                    update_logfile_path=Mock(return_value='updated.log'),
                ):
                    app.run_main_flow(args, logger, 'initial.log')
                select_task.assert_not_called()
                if expected_selection is None:
                    select_container.assert_not_called()
                else:
                    select_container.assert_called_once_with(logger, 'cluster', 'explicit', *expected_selection)
                self.assertEqual(execute.call_args.args[3:5], ('explicit', 'web' if expected_selection is None else 'app'))


if __name__ == '__main__':
    unittest.main()
